//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! Versioned codec seam. A [`VersionedCodec`] type can be decoded from any
//! supported version (up-converting older layouts into the current in-memory
//! type) and encoded at any active version (down-converting the current type
//! into an older layout). [`encode_framed`] / [`decode_framed`] add and validate
//! the leading version byte; [`encode_postcard`] / [`decode_postcard_exact`] are
//! the DRY per-version body building blocks implementors call.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::CodecError;

/// A type whose persisted and on-wire representation is versioned.
///
/// `decode_version` up-converts an older layout into the current in-memory type;
/// `encode_version` down-converts the current type into the layout of a given
/// (possibly older) version. Implementors typically `match` on the version,
/// routing to a per-version struct via [`decode_postcard_exact`] /
/// [`encode_postcard`] and a `From` conversion. Both methods operate on the
/// bare body (the bytes after the version prefix); framing is handled by
/// [`encode_framed`] / [`decode_framed`].
pub trait VersionedCodec: Sized {
    /// Decode a `body` known to be `version` into the current type.
    fn decode_version(version: u8, body: &[u8]) -> Result<Self, CodecError>;

    /// Encode `self` as the body layout of `version`.
    fn encode_version(&self, version: u8) -> Result<Vec<u8>, CodecError>;
}

/// Encode `value` as `[version | body]`, where `body` is `value`'s layout at
/// `version` (see [`VersionedCodec::encode_version`]).
pub fn encode_framed<T: VersionedCodec>(version: u8, value: &T) -> Result<Vec<u8>, CodecError> {
    let body = value.encode_version(version)?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(version);
    out.extend_from_slice(&body);
    Ok(out)
}

/// Decode a `[version | body]` payload, rejecting a version outside the reader's
/// supported range `[min_version, max_version]` with [`CodecError::VersionUnsupported`]
/// before any body parse. An in-range version is dispatched to
/// [`VersionedCodec::decode_version`], up-converting older layouts into the
/// current type.
pub fn decode_framed<T: VersionedCodec>(
    min_version: u8,
    max_version: u8,
    bytes: &[u8],
) -> Result<T, CodecError> {
    let (first, body) = bytes.split_first().ok_or(CodecError::Empty)?;
    let version = *first;
    if version < min_version || version > max_version {
        return Err(CodecError::VersionUnsupported {
            min: min_version,
            max: max_version,
            actual: version,
        });
    }
    T::decode_version(version, body)
}

/// Serialize `value` to a bare postcard body (no version prefix). Used by
/// [`VersionedCodec::encode_version`] implementations per version.
pub fn encode_postcard<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    postcard::to_stdvec(value).map_err(CodecError::Encode)
}

/// Deserialize a bare postcard body, rejecting surplus trailing bytes. A
/// versioned format that exists to catch drift treats trailing garbage as
/// corruption (e.g. a partial overwrite), not a clean record. Used by
/// [`VersionedCodec::decode_version`] implementations per version.
pub fn decode_postcard_exact<T: DeserializeOwned>(body: &[u8]) -> Result<T, CodecError> {
    let (value, remainder) = postcard::take_from_bytes(body).map_err(CodecError::Decode)?;
    if !remainder.is_empty() {
        return Err(CodecError::TrailingBytes {
            extra: remainder.len(),
        });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Body {
        idx: u64,
        name: String,
    }

    // Current in-memory type. `color` is added at v2 and, per the design's
    // representability rule, is feature-gated off (None) until the active write
    // version reaches 2, so down-conversion to v1 is always lossless.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Widget {
        id: u64,
        label: String,
        color: Option<String>,
    }

    // v1 on-disk/wire layout: no `color`.
    #[derive(Serialize, Deserialize)]
    struct WidgetV1 {
        id: u64,
        label: String,
    }

    // v2 layout: includes `color`.
    #[derive(Serialize, Deserialize)]
    struct WidgetV2 {
        id: u64,
        label: String,
        color: Option<String>,
    }

    impl From<WidgetV1> for Widget {
        fn from(widget: WidgetV1) -> Self {
            Widget {
                id: widget.id,
                label: widget.label,
                color: None,
            }
        }
    }

    impl From<WidgetV2> for Widget {
        fn from(widget: WidgetV2) -> Self {
            Widget {
                id: widget.id,
                label: widget.label,
                color: widget.color,
            }
        }
    }

    const WIDGET_MIN: u8 = 1;
    const WIDGET_MAX: u8 = 2;

    impl VersionedCodec for Widget {
        fn decode_version(version: u8, body: &[u8]) -> Result<Self, CodecError> {
            match version {
                1 => decode_postcard_exact::<WidgetV1>(body).map(Into::into),
                2 => decode_postcard_exact::<WidgetV2>(body).map(Into::into),
                other => Err(CodecError::VersionUnsupported {
                    min: WIDGET_MIN,
                    max: WIDGET_MAX,
                    actual: other,
                }),
            }
        }

        fn encode_version(&self, version: u8) -> Result<Vec<u8>, CodecError> {
            match version {
                1 => {
                    // v1 has no `color`. If it is set (a feature-gate violation —
                    // `color` belongs to v2+), fail loudly rather than silently
                    // drop it. encode_version returns Result precisely for this.
                    if self.color.is_some() {
                        return Err(CodecError::NotRepresentable { version: 1 });
                    }
                    encode_postcard(&WidgetV1 {
                        id: self.id,
                        label: self.label.clone(),
                    })
                }
                2 => encode_postcard(&WidgetV2 {
                    id: self.id,
                    label: self.label.clone(),
                    color: self.color.clone(),
                }),
                other => Err(CodecError::VersionUnsupported {
                    min: WIDGET_MIN,
                    max: WIDGET_MAX,
                    actual: other,
                }),
            }
        }
    }

    #[test]
    fn postcard_body_roundtrips() {
        let original = Body {
            idx: 7,
            name: "ts".into(),
        };
        let bytes = encode_postcard(&original).expect("encode");
        let decoded: Body = decode_postcard_exact(&bytes).expect("decode");
        assert_eq!(original, decoded);
    }

    #[test]
    fn postcard_body_rejects_trailing_bytes() {
        let mut bytes = encode_postcard(&Body {
            idx: 1,
            name: "x".into(),
        })
        .expect("encode");
        bytes.extend_from_slice(&[0xAB, 0xCD]);
        assert!(matches!(
            decode_postcard_exact::<Body>(&bytes),
            Err(CodecError::TrailingBytes { extra: 2 })
        ));
    }

    #[test]
    fn version_unsupported_carries_range_and_actual() {
        let err = CodecError::VersionUnsupported {
            min: 1,
            max: 2,
            actual: 9,
        };
        assert_eq!(
            err.to_string(),
            "version unsupported: 9 outside readable range [1, 2]"
        );
    }

    #[test]
    fn not_representable_names_the_version() {
        let err = CodecError::NotRepresentable { version: 1 };
        assert_eq!(err.to_string(), "value not representable at version 1");
    }

    #[test]
    fn trait_encode_decode_roundtrips_each_version() {
        let widget = Widget {
            id: 42,
            label: "tso".into(),
            color: Some("amber".into()),
        };
        let body_v2 = widget.encode_version(2).expect("encode v2");
        let decoded_v2 = Widget::decode_version(2, &body_v2).expect("decode v2");
        assert_eq!(widget, decoded_v2);

        let gated = Widget {
            id: 7,
            label: "old".into(),
            color: None,
        };
        let body_v1 = gated.encode_version(1).expect("encode v1");
        let decoded_v1 = Widget::decode_version(1, &body_v1).expect("decode v1");
        assert_eq!(gated, decoded_v1);
    }

    #[test]
    fn down_convert_rejects_non_representable_state() {
        // A v2-only field (color) set while encoding the v1 layout must fail
        // loud, not silently drop — in release builds too.
        let widget = Widget {
            id: 1,
            label: "x".into(),
            color: Some("blue".into()),
        };
        assert!(matches!(
            widget.encode_version(1),
            Err(CodecError::NotRepresentable { version: 1 })
        ));
    }

    #[test]
    fn encode_framed_prepends_version_and_matches_body() {
        let widget = Widget {
            id: 5,
            label: "z".into(),
            color: None,
        };
        let framed = encode_framed(1, &widget).expect("encode_framed v1");
        assert_eq!(framed[0], 1, "leading byte is the version");
        // The remainder must equal the bare v1 body — proving no `color` byte
        // leaks into the v1 layout (down-conversion is lossless).
        let expected_body = widget.encode_version(1).expect("encode v1 body");
        assert_eq!(&framed[1..], expected_body.as_slice());
    }

    #[test]
    fn decode_framed_dispatches_and_up_converts() {
        // A record written at v1 (no color) decodes into the current type with
        // color defaulted to None.
        let v1_body = encode_postcard(&WidgetV1 {
            id: 9,
            label: "old".into(),
        })
        .expect("body");
        let mut framed = vec![1u8];
        framed.extend_from_slice(&v1_body);
        let widget: Widget = decode_framed(WIDGET_MIN, WIDGET_MAX, &framed).expect("decode v1");
        assert_eq!(
            widget,
            Widget {
                id: 9,
                label: "old".into(),
                color: None
            }
        );
    }

    #[test]
    fn decode_framed_roundtrips_current_version() {
        let widget = Widget {
            id: 1,
            label: "now".into(),
            color: Some("red".into()),
        };
        let framed = encode_framed(2, &widget).expect("encode");
        let back: Widget = decode_framed(WIDGET_MIN, WIDGET_MAX, &framed).expect("decode");
        assert_eq!(widget, back);
    }

    #[test]
    fn decode_framed_rejects_out_of_range_version() {
        let framed = [3u8, 0x00];
        let err = decode_framed::<Widget>(WIDGET_MIN, WIDGET_MAX, &framed).expect_err("reject");
        assert!(matches!(
            err,
            CodecError::VersionUnsupported {
                min: 1,
                max: 2,
                actual: 3
            }
        ));
    }

    #[test]
    fn decode_framed_rejects_below_range_version() {
        let framed = [0u8, 0x00];
        let err = decode_framed::<Widget>(WIDGET_MIN, WIDGET_MAX, &framed).expect_err("reject");
        assert!(matches!(
            err,
            CodecError::VersionUnsupported {
                min: 1,
                max: 2,
                actual: 0
            }
        ));
    }

    #[test]
    fn decode_framed_rejects_empty() {
        let err = decode_framed::<Widget>(WIDGET_MIN, WIDGET_MAX, &[]).expect_err("reject");
        assert!(matches!(err, CodecError::Empty));
    }

    use proptest::prelude::*;

    proptest! {
        // For any widget with color gated off, framing at v1 then decoding
        // returns the original; framing at v2 with any color round-trips too.
        #[test]
        fn framed_roundtrips_any(id in any::<u64>(), label in any::<String>(), color in any::<Option<String>>()) {
            let gated = Widget { id, label: label.clone(), color: None };
            let framed_v1 = encode_framed(1, &gated).unwrap();
            let back_v1: Widget = decode_framed(WIDGET_MIN, WIDGET_MAX, &framed_v1).unwrap();
            prop_assert_eq!(gated, back_v1);

            let full = Widget { id, label, color };
            let framed_v2 = encode_framed(2, &full).unwrap();
            let back_v2: Widget = decode_framed(WIDGET_MIN, WIDGET_MAX, &framed_v2).unwrap();
            prop_assert_eq!(full, back_v2);
        }
    }
}
