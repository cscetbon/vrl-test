use log::{debug, warn};
use vrl::prelude::*;
use vrl::compiler::compile;
use std::time::Instant;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn split(value: Value, pattern: Value) -> Resolved {
    let string = value.try_bytes_utf8_lossy()?;
    let result = match pattern {
        Value::Regex(pattern) => pattern.splitn(string.as_ref(), value.to_string().len()).collect::<Vec<_>>(),
        Value::Bytes(bytes) => {
            let pattern = String::from_utf8_lossy(&bytes);
            string.splitn(value.to_string().len(), pattern.as_ref()).collect::<Vec<_>>()
        }
        value => {
            return Err(ValueError::Expected {
                got: value.kind(),
                expected: Kind::regex() | Kind::bytes(),
            }
            .into())
        }
    };

    if result.len() == 1 && result[0].is_empty() {
        Ok(Value::Array(vec![]))
    } else {
        Ok(result.into())
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

        Ok(SplitFn {
            value,
            pattern,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SplitFn {
    value: Box<dyn Expression>,
    pattern: Box<dyn Expression>,
}

impl FunctionExpression for SplitFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let pattern = self.pattern.resolve(ctx)?;

        split(value, pattern)
    }

    fn type_def(&self, _: &state::TypeState) -> TypeDef {
        TypeDef::array(Collection::from_unknown(Kind::bytes())).infallible()
    }
}

fn main() {
    let mut functions = vrl::stdlib::all();
    // Replace function with identifier "split" by our new function
    let split = functions.iter_mut().find(|f| f.identifier() == "split").unwrap();
    *split = Box::new(Split) as _;
    // println!("functions: {:?}", functions);
    let program = "split(\"a,b,c\", \",\")";
    let start = Instant::now();
    match compile(&program, &functions) {
        Ok(result) => {
            debug!(
                "Compiled a vrl program ({}), took {:?}",
                program
                    .lines()
                    .into_iter()
                    .skip(1)
                    .next()
                    .unwrap_or("expansion"),
                start.elapsed()
            );
            if result.warnings.len() > 0 {
                warn!("{:?}", result.warnings);
            }
        }
        Err(_diagnostics) => {
        }
    }

    // print test of split with value "a,b,c" and pattern ","
    let test = crate::split("a,b,c".into(), ",".into());
    println!("test: {:?}", test);
}

#[cfg(test)]
#[allow(clippy::trivial_regex)]
mod test {
    use super::*;
    use paste::paste;

    macro_rules! split_test {
        ($name:ident, $input:expr, $pattern:expr, $expected:expr) => {
            paste! {
                #[test]
                fn [<split_ $name>]() {
                    let expression = crate::split(
                        $input.into(), $pattern.into(),
                    ).unwrap();
                    let expected: Vec<Value> = $expected.iter().map(|&s: &&str| s.into()).collect();
                    assert_eq!(expression, Value::Array(expected));
                }
            }
        };
    }

    split_test!(empty, "", ",", [""; 0]);
    split_test!(single, "foo", ",", ["foo"]);
    split_test!(long, "This is a long string.", " ", ["This", "is", "a", "long", "string."]);

}