use structopt::*;

#[derive(StructOpt, Debug)]
pub struct Config {
    #[structopt(short, long, env="GIRASOL_HOME")]
    pub home: String
}