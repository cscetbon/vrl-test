use log::{debug, warn};
use serde_json::json;
use std::collections::BTreeMap;
use vrl::compiler::runtime::Runtime;
use vrl::compiler::TimeZone;
use vrl::compiler::{compile, TargetValue};
use vrl::prelude::*;
use vrl::value::{Secrets, Value};

fn split(value: Value, limit: Value, pattern: Value) -> Resolved {
    let string = value.try_bytes_utf8_lossy()?;
    let limit = match limit.try_integer()? {
        x if x < 0 => 0,
        x => x as usize,
    };
    match pattern {
        Value::Regex(pattern) => Ok(pattern
            .splitn(string.as_ref(), limit)
            .collect::<Vec<_>>()
            .into()),
        Value::Bytes(bytes) => {
            let pattern = String::from_utf8_lossy(&bytes);

            Ok(string
                .splitn(limit, pattern.as_ref())
                .collect::<Vec<_>>()
                .into())
        }
        value => Err(ValueError::Expected {
            got: value.kind(),
            expected: Kind::regex() | Kind::bytes(),
        }
        .into()),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Split;

impl Function for Split {
    fn identifier(&self) -> &'static str {
        "split"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "pattern",
                kind: kind::BYTES | kind::REGEX,
                required: true,
            },
            Parameter {
                keyword: "limit",
                kind: kind::INTEGER,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "split string",
                source: r#"split("foobar", "b")"#,
                result: Ok(r#"["foo", "ar"]"#),
            },
            Example {
                title: "split once",
                source: r#"split("foobarbaz", "ba", 2)"#,
                result: Ok(r#"["foo", "rbaz"]"#),
            },
            Example {
                title: "split regex",
                source: r#"split("barbaz", r'ba')"#,
                result: Ok(r#"["", "r", "z"]"#),
            },
        ]
    }

    fn compile(
        &self,
        _state: &state::TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        let pattern = arguments.required("pattern");
        let limit = arguments.optional("limit").unwrap_or(expr!(999_999_999));

        Ok(SplitFn {
            value,
            pattern,
            limit,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SplitFn {
    value: Box<dyn Expression>,
    pattern: Box<dyn Expression>,
    limit: Box<dyn Expression>,
}

impl FunctionExpression for SplitFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let limit = self.limit.resolve(ctx)?;
        let pattern = self.pattern.resolve(ctx)?;

        split(value, limit, pattern)
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::array(Collection::from_unknown(Kind::bytes())).infallible()
    }
}

fn main() {
    // Initialize the logger
    env_logger::init();
    let functions = vrl::stdlib::all();
    // let mut functions = vrl::stdlib::all();
    // Replace function with identifier "split" by our new function
    // let split = functions
    //     .iter_mut()
    //     .find(|f| f.identifier() == "split")
    //     .unwrap();
    // *split = Box::new(Split) as _;
    // println!("functions: {:?}", functions);
    let program = r#"
        # Remove some fields
        del(.foo)

        # Add a timestamp
        .timestamp = now()

        # Parse HTTP status code into local variable
        http_status_code = parse_int!(.http_status)
        del(.http_status)

        # Add status
        if http_status_code >= 200 && http_status_code <= 299 {
            .status = "success"
        } else {
            .status = "error"
        }
    "#;

    println!("program: {:?}", program);

    // Compile the VRL script
    let program = compile(&program, &functions)
        .map_err(|diagnostics| {
            println!("Error compiling program: {:?}", diagnostics);
            diagnostics
        })
        .unwrap();

    if !program.warnings.is_empty() {
        warn!("{:?}", program.warnings);
    }

    let mut runtime = Runtime::default();
    let timezone = TimeZone::default();

    let event = Value::from(json!(
        {
            "message": "Hello VRL",
            "foo": "delete me",
            "http_status": "200"
        }
    ));

    println!("{:?}", event.clone());

    let mut target_value = TargetValue {
        value: event,
        metadata: Value::Object(BTreeMap::new()),
        secrets: Secrets::new(),
    };

    match runtime.resolve(&mut target_value, &program.program, &timezone) {
        Ok(_) => {
            debug!("Resolved event: {:?}", target_value.value);
        }
        Err(e) => {
            println!("Error resolving event: {:?}", e);
        }
    }

    // // Convert the transformed Value back into a Rust struct
    // let transformed: Input = serde_json::from_value(message_value.to_json()?)?;

    // let program = r#"
    //     .e = split("a,b,c", ",");
    // "#;
    // let start = Instant::now();
    // match compile(&program, &functions) {
    //     Ok(result) => {
    //         debug!(
    //             "Compiled a vrl program ({}), took {:?}",
    //             program
    //                 .lines()
    //                 .into_iter()
    //                 .skip(1)
    //                 .next()
    //                 .unwrap_or("expansion"),
    //             start.elapsed()
    //         );
    //         if result.warnings.len() > 0 {
    //             warn!("{:?}", result.warnings);
    //         }
    //     }
    //     Err(_diagnostics) => {
    //         println!("Error compiling program: {:?}", _diagnostics);
    //     }
    // }

    // print test of split with value "a,b,c" and pattern ","
    // let test = crate::split("a,b,c".into(), ",".into(), 999_999_999.into());
    // println!("test: {:?}", test);
}

#[cfg(test)]
#[allow(clippy::trivial_regex)]
mod test {
    use super::*;
    use vrl::value;
    // use vrl::prelude::test_function;

    test_function![
        split => Split;

        empty {
            args: func_args![value: "",
                             pattern: " "
            ],
            want: Ok(value!([""])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        single {
            args: func_args![value: "foo",
                             pattern: " "
            ],
            want: Ok(value!(["foo"])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        long {
            args: func_args![value: "This is a long string.",
                             pattern: " "
            ],
            want: Ok(value!(["This", "is", "a", "long", "string."])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        regex {
            args: func_args![value: "This is a long string",
                             pattern: Value::Regex(regex::Regex::new(" ").unwrap().into()),
                             limit: 2
            ],
            want: Ok(value!(["This", "is a long string"])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        non_space {
            args: func_args![value: "ThisaisAlongAstring.",
                             pattern: Value::Regex(regex::Regex::new("(?i)a").unwrap().into())
            ],
            want: Ok(value!(["This", "is", "long", "string."])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        unicode {
             args: func_args![value: "˙ƃuᴉɹʇs ƃuol ɐ sᴉ sᴉɥ┴",
                              pattern: " "
             ],
             want: Ok(value!(["˙ƃuᴉɹʇs", "ƃuol", "ɐ", "sᴉ", "sᴉɥ┴"])),
             tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
         }

        limit {
            args: func_args![value: "This is a long string.",
                             pattern: " ",
                             limit: 2
            ],
            want: Ok(value!(["This", "is a long string."])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        over_length_limit {
            args: func_args![value: "This is a long string.",
                             pattern: " ",
                             limit: 2000
            ],
            want: Ok(value!(["This", "is", "a", "long", "string."])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        zero_limit {
            args: func_args![value: "This is a long string.",
                             pattern: " ",
                             limit: 0
            ],
            want: Ok(value!([])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }

        negative_limit {
            args: func_args![value: "This is a long string.",
                             pattern: " ",
                             limit: -1
            ],
            want: Ok(value!([])),
            tdef: TypeDef::array(Collection::from_unknown(Kind::bytes())),
        }
    ];
}
