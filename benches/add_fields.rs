//! This mod provides useful utilities for writing benchmarks.

use criterion::{
    criterion_group, criterion_main, measurement::Measurement, BatchSize, Bencher, BenchmarkGroup,
    BenchmarkId, Criterion, Throughput,
};

use serde::{Deserialize, Serialize};
use vector::{config::TransformConfig, transforms::Transform};
use vector::{transforms::FunctionTransform, Event};
use vector_test_framework::hello;

criterion_group!(benches, benchmark);
criterion_main!(benches);

trait BenchmarkGroupExt {
    fn bench_function_transform<ID: Into<String>>(
        &mut self,
        id: ID,
        toml_config: &str,
        prewarm_events: Vec<Event>,
        events: Vec<Event>,
    ) -> &mut Self;
}

impl<'a, M: Measurement> BenchmarkGroupExt for BenchmarkGroup<'a, M> {
    fn bench_function_transform<ID>(
        &mut self,
        id: ID,
        toml_config: &str,
        prewarm_events: Vec<Event>,
        events: Vec<Event>,
    ) -> &mut Self
    where
        ID: Into<String>,
    {
        hello();
        let transform_config = parse_transform_config(toml_config);
        let transform = build_transform(transform_config.as_ref());
        let transform_function = transform.into_function();

        self.throughput(Throughput::Elements(events.len() as u64));
        self.bench_function(
            BenchmarkId::new(
                format!("transform/{}", transform_config.transform_type()),
                id.into(),
            ),
            move |b| {
                run_function_transform(
                    b,
                    transform_function.clone(),
                    prewarm_events.clone(),
                    events.clone(),
                );
            },
        );

        self
    }
}

pub fn run_function_transform<M: Measurement>(
    b: &mut Bencher<'_, M>,
    component: Box<dyn FunctionTransform>,
    prewarm_events: Vec<Event>,
    events: Vec<Event>,
) {
    b.iter_batched(
        || {
            let mut out: Vec<Event> = Vec::with_capacity(1);
            let mut component = component.clone();
            for event in prewarm_events.clone() {
                component.transform(&mut out, event);
                out.clear();
            }
            let events = events.clone();
            (component, out, events)
        },
        |(mut component, mut out, events)| {
            for event in events {
                component.transform(&mut out, event);
                out.clear();
            }
            out
        },
        BatchSize::SmallInput,
    )
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TransformParser {
    #[serde(flatten)]
    pub inner: Box<dyn TransformConfig>,
}

fn parse_transform_config(toml_config: &str) -> Box<dyn TransformConfig> {
    toml::from_str(toml_config).expect("you must pass a valid config in benches")
}

fn build_transform(transform_config: &dyn TransformConfig) -> Transform {
    futures::executor::block_on(transform_config.build()).expect("transform must build")
}

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("test");

    group.bench_function_transform(
        "single_field",
        r#"
            type = "add_fields"
            fields.a = "b"
            overwrite = false
        "#,
        vec![Event::new_empty_log()],
        vec![Event::new_empty_log()],
    );

    group.bench_function_transform(
        "two_fields",
        r#"
            type = "add_fields"
            fields.a = "b"
            fields.d = "c"
            overwrite = false
        "#,
        vec![Event::new_empty_log()],
        vec![Event::new_empty_log()],
    );

    group.bench_function_transform(
        "three_fields",
        r#"
            type = "add_fields"
            fields.a = "b"
            fields.d = "c"
            fields.e = "f"
            overwrite = false
        "#,
        vec![Event::new_empty_log()],
        vec![Event::new_empty_log()],
    );

    group.bench_function_transform(
        "ten_events",
        r#"
            type = "add_fields"
            fields.a = "b"
            fields.d = "c"
            fields.e = "f"
            overwrite = false
        "#,
        std::iter::repeat(Event::new_empty_log()).take(10).collect(),
        std::iter::repeat(Event::new_empty_log()).take(10).collect(),
    );

    group.bench_function_transform(
        "non_empty_events",
        r#"
            type = "add_fields"
            fields.a = "b"
            fields.d = "c"
            fields.e = "f"
            overwrite = false
        "#,
        std::iter::repeat(Event::new_empty_log()).take(10).collect(),
        std::iter::repeat(Event::new_empty_log()).take(10).collect(),
    );

    group.finish();
}

// mod testevent {
//     use std::iter::FromIterator;

//     pub fn gen<T: FromIterator<Item = Event>>(amount: usize) -> T {}
// }
