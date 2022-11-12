use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/pg_logicaldec.proto"], &["src/"])?;
    Ok(())
}
