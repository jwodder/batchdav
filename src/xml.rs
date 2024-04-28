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

use crate::types::DirectoryListing;
use bytes::{Buf, Bytes};
use thiserror::Error;
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
    OpenForeign(String),
    CloseForeign(String),
    Text(String),
}

impl Token {
    fn open(name: String, namespace: Option<String>) -> Token {
        if namespace.map_or(true, |ns| ns == DAV_XMLNS) {
            Token::OpenDav(name)
        } else {
            Token::OpenForeign(name)
        }
    }

    fn close(name: String, namespace: Option<String>) -> Token {
        if namespace.map_or(true, |ns| ns == DAV_XMLNS) {
            Token::CloseDav(name)
        } else {
            Token::CloseForeign(name)
        }
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

fn parse(tokens: Vec<Token>) -> Result<DirectoryListing<String>, FromXmlError> {
    todo!()
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum FromXmlError {
    #[error(transparent)]
    Tokenize(#[from] XmlTokenizeError),
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
