use crate::types::DirectoryListing;
use bytes::{Buf, Bytes};
use thiserror::Error;
use winnow::{
    combinator::{delimited, opt, preceded, repeat, seq},
    error::{ContextError, ErrMode, ErrorKind, ParserError},
    stream::{Compare, CompareResult, ContainsToken, SliceLen},
    token::literal,
    PResult, Parser,
};
use xml::reader::{Error as XmlError, ParserConfig2, XmlEvent};

/// The XML namespace for standard WebDAV elements
static DAV_XMLNS: &str = "DAV:";

pub(crate) fn parse_multistatus(
    blob: Bytes,
    charset: Option<String>,
) -> Result<DirectoryListing<String>, FromXmlError> {
    parse(tokenize(blob, charset)?)
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Token {
    OpenDav(String),
    CloseDav(String),
    OpenExt { name: String, namespace: String },
    CloseExt { name: String, namespace: String },
    Text(String),
}

impl Token {
    fn open(name: String, namespace: Option<String>) -> Token {
        match namespace {
            Some(ns) if ns == DAV_XMLNS => Token::OpenDav(name),
            None => Token::OpenDav(name),
            Some(namespace) => Token::OpenExt { name, namespace },
        }
    }

    fn close(name: String, namespace: Option<String>) -> Token {
        match namespace {
            Some(ns) if ns == DAV_XMLNS => Token::CloseDav(name),
            None => Token::CloseDav(name),
            Some(namespace) => Token::CloseExt { name, namespace },
        }
    }
}

impl Compare<Token> for &[Token] {
    #[inline]
    fn compare(&self, t: Token) -> CompareResult {
        match self.first() {
            Some(c) if &t == c => CompareResult::Ok(1),
            Some(_) => CompareResult::Error,
            None => CompareResult::Incomplete,
        }
    }
}

impl ContainsToken<Token> for Token {
    #[inline(always)]
    fn contains_token(&self, token: Token) -> bool {
        *self == token
    }
}

impl ContainsToken<Token> for &[Token] {
    #[inline]
    fn contains_token(&self, token: Token) -> bool {
        self.iter().any(|t| *t == token)
    }
}

impl<const N: usize> ContainsToken<Token> for [Token; N] {
    #[inline]
    fn contains_token(&self, token: Token) -> bool {
        self.iter().any(|t| *t == token)
    }
}

impl<const N: usize> ContainsToken<Token> for &[Token; N] {
    #[inline]
    fn contains_token(&self, token: Token) -> bool {
        self.iter().any(|t| *t == token)
    }
}

impl SliceLen for Token {
    fn slice_len(&self) -> usize {
        1
    }
}

fn tokenize(blob: Bytes, charset: Option<String>) -> Result<Vec<Token>, XmlTokenizeError> {
    let encoding = charset.and_then(|cs| cs.parse::<xml::Encoding>().ok());
    let reader = ParserConfig2::new()
        .ignore_invalid_encoding_declarations(false)
        .override_encoding(encoding)
        .allow_multiple_root_elements(false)
        .trim_whitespace(true)
        .create_reader(blob.reader());
    let mut tokens = Vec::new();
    for event in reader {
        use XmlEvent::*;
        match event? {
            StartElement { name, .. } => tokens.push(Token::open(name.local_name, name.namespace)),
            EndElement { name, .. } => tokens.push(Token::close(name.local_name, name.namespace)),
            CData(s) | Characters(s) => tokens.push(Token::Text(s)),
            StartDocument { .. } | EndDocument | Comment(..) | Whitespace(..) => (),
            ProcessingInstruction { .. } => return Err(XmlTokenizeError::ProcessingInstruction),
        }
    }
    Ok(tokens)
}

/*
For reference: Relevant DTD fragments from
<http://www.webdav.org/specs/rfc4918.html#xml.element.definitions>:

    <!ELEMENT multistatus (response*, responsedescription?)>
    <!ELEMENT response (href, ((href*, status)|(propstat+)),
                        error?, responsedescription?, location?)>
    <!ELEMENT href (#PCDATA)>
    <!ELEMENT propstat (prop, status, error?, responsedescription?)>
    <!ELEMENT prop ANY>
    <!ELEMENT status (#PCDATA)>
    <!ELEMENT error ANY>
    <!ELEMENT responsedescription (#PCDATA)>
    <!ELEMENT location (href)>
*/

fn parse(tokens: Vec<Token>) -> Result<DirectoryListing<String>, FromXmlError> {
    let (responses,): (Vec<Response>,) = seq!(
        _: open("multistatus"),
        repeat(0.., preceded(extensions, response)),
        _: extensions,
        _: close("multistatus"),
    )
    .parse(tokens.as_slice())
    .map_err(|_| FromXmlError::Parse)?;
    let mut directories = Vec::new();
    let mut files = Vec::new();
    for r in responses {
        if !is_ok(&r.status) {
            return Err(FromXmlError::BadStatus {
                href: r.href,
                status: r.status,
            });
        }
        if r.is_collection {
            directories.push(r.href);
        } else {
            files.push(r.href);
        }
    }
    Ok(DirectoryListing { directories, files })
}

//type TokenStream<'a> = Located<&'a [Token]>;
type TokenStream<'a> = &'a [Token];

fn open<'a, E: ParserError<TokenStream<'a>>>(
    name: &'static str,
) -> impl Parser<TokenStream<'a>, TokenStream<'a>, E> {
    literal(Token::OpenDav(name.to_owned()))
}

fn close<'a, E: ParserError<TokenStream<'a>>>(
    name: &'static str,
) -> impl Parser<TokenStream<'a>, TokenStream<'a>, E> {
    literal(Token::CloseDav(name.to_owned()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Response {
    href: String,
    is_collection: bool,
    status: String,
}

fn response(input: &mut TokenStream<'_>) -> PResult<Response> {
    // TODO: Support (href*, status) in lieu of propstat+ (What does the former
    // even mean?)
    let (href, propstats): (_, Vec<Propstat>) = seq!(
        _: open("response"),
        _: extensions,
        href_tag,
        repeat(1.., preceded(extensions, propstat)),
        _: extensions,
        // <error> tags cannot be present in responses to the kind of requests
        // we're making, so don't bother with them.
        //_: opt(error_tag),
        //_: extensions,
        _: opt(responsedescription),
        _: extensions,
        _: opt(location),
        _: extensions,
        _: close("response"),
    )
    .parse_next(input)?;
    if let Some((is_collection, status)) = propstats
        .into_iter()
        .find_map(|ps| ps.is_collection.map(|c| (c, ps.status)))
    {
        Ok(Response {
            href,
            is_collection,
            status,
        })
    } else {
        hard_fail(input)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Propstat {
    is_collection: Option<bool>,
    status: String,
}

fn propstat(input: &mut TokenStream<'_>) -> PResult<Propstat> {
    let (prop, status) = seq!(
        _: open("propstat"),
        _: extensions,
        prop_tag,
        _: extensions,
        status_tag,
        _: extensions,
        // <error> tags cannot be present in responses to the kind of requests
        // we're making, so don't bother with them.
        //_: opt(error_tag),
        //_: extensions,
        _: opt(responsedescription),
        _: extensions,
        _: close("propstat"),
    )
    .parse_next(input)?;
    Ok(Propstat {
        is_collection: Some(prop.is_collection),
        status,
    })
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Prop {
    is_collection: bool,
}

// Note: When parsing <prop>, we're assuming that the only property requested
// by the client was <resourcetype>, so this part of the code expects <prop> to
// contain that tag and only that tag.  (If the client is ever adjusted to
// request more properties, this is where to start updating the XML-parsing
// code.)
fn prop_tag(input: &mut TokenStream<'_>) -> PResult<Prop> {
    let (is_collection,) = seq!(
        _: open("prop"),
        _: open("resourcetype"),
        opt((open("collection"), close("collection"))).map(|o| o.is_some()),
        _: close("resourcetype"),
        _: close("prop"),
    )
    .parse_next(input)?;
    Ok(Prop { is_collection })
}

fn href_tag(input: &mut TokenStream<'_>) -> PResult<String> {
    delimited(open("href"), text, close("href")).parse_next(input)
}

fn status_tag(input: &mut TokenStream<'_>) -> PResult<String> {
    delimited(open("status"), text, close("status")).parse_next(input)
}

fn responsedescription(input: &mut TokenStream<'_>) -> PResult<()> {
    delimited(
        open("responsedescription"),
        text,
        close("responsedescription"),
    )
    .void()
    .parse_next(input)
}

fn location(input: &mut TokenStream<'_>) -> PResult<()> {
    seq!(
        open("location"),
        extensions,
        href_tag,
        extensions,
        close("location"),
    )
    .void()
    .parse_next(input)
}

fn extensions(input: &mut TokenStream<'_>) -> PResult<()> {
    let mut tag_stack = Vec::new();
    let mut i = 0;
    for t in *input {
        match t {
            Token::OpenExt { name, namespace } => tag_stack.push((name, namespace)),
            Token::CloseExt { name, namespace } => {
                if tag_stack.last() == Some(&(name, namespace)) {
                    tag_stack.pop();
                } else {
                    return hard_fail(input);
                }
            }
            Token::Text(_) => {
                if tag_stack.is_empty() {
                    return hard_fail(input);
                }
            }
            _ => {
                if tag_stack.is_empty() {
                    break;
                } else {
                    return hard_fail(input);
                }
            }
        }
        i += 1;
    }
    *input = &input[i..];
    Ok(())
}

#[allow(clippy::unnecessary_wraps)]
fn text(input: &mut TokenStream<'_>) -> PResult<String> {
    let mut s = String::new();
    let mut i = 0;
    for t in *input {
        if let Token::Text(ref tt) = t {
            s.push_str(tt);
        } else {
            break;
        }
        i += 1;
    }
    *input = &input[i..];
    Ok(s)
}

fn hard_fail<T>(input: TokenStream<'_>) -> PResult<T> {
    Err(ErrMode::Cut(ContextError::from_error_kind(
        &input,
        ErrorKind::Fail,
    )))
}

fn is_ok(s: &str) -> bool {
    let mut words = s.split_ascii_whitespace();
    let Some(http_version) = words.next() else {
        return false;
    };
    if !http_version.starts_with("HTTP/") {
        return false;
    }
    let Some(status) = words.next() else {
        return false;
    };
    status == "200"
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum FromXmlError {
    #[error(transparent)]
    Tokenize(#[from] XmlTokenizeError),
    #[error("XML response is not valid")]
    Parse,
    #[error("resourcetype status for {href:?} is not OK: {status:?}")]
    BadStatus { href: String, status: String },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum XmlTokenizeError {
    #[error("error tokenizing XML")]
    Xml(#[from] XmlError),
    #[error("unexpected XML processing instruction encountered")]
    ProcessingInstruction,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test01() {
        let bs = include_bytes!("testdata/response.xml");
        let dl = parse_multistatus(Bytes::from(bs.as_slice()), None).unwrap();
        assert_eq!(dl, DirectoryListing {
            directories: vec![
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/0/".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/1/".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/2/".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/3/".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/4/".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/5/".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/6/".into(),
            ],
            files: vec![
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/.zattrs".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/.zgroup".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/.zmetadata".into(),
                "/zarrs/0d5/b9b/0d5b9be5-e626-4f6a-96da-b6b602954899/0395d0a3767524377b58da3945b3c063-48379--27115470.zarr/info".into(),
            ],
        });
    }
}
