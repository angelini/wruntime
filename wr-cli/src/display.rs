use tabled::{builder::Builder, settings::Style};

pub fn print_table(builder: Builder) {
    let mut table = builder.build();
    table.with(Style::rounded());
    println!("{table}");
}
