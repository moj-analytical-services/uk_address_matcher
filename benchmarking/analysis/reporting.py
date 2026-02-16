from __future__ import annotations


def print_benchmark(dataset_name: str, variant_name: str) -> None:
    """Print a clear header for benchmark run.

    Parameters
    ----------
    dataset_name:
        Name of the dataset being benchmarked.
    variant_name:
        Name of the pipeline variant being run.
    enabled_stages:
        List of enabled stages, or None if only default stages.
    """
    print("\n" + "=" * 80)
    print(f"BENCHMARK RUN: {variant_name}")
    print("=" * 80)
    print(f"Dataset: {dataset_name}")


def _format_stage_name(stage: object) -> str:
    if hasattr(stage, "__class__"):
        return stage.__class__.__name__
    return str(stage)


def print_stages_benchmark_header(
    dataset_name: str, variant_name: str, stages: list | None = None
) -> None:
    """Print a clear header for benchmark run.

    Parameters
    ----------
    dataset_name:
        Name of the dataset being benchmarked.
    variant_name:
        Name of the pipeline variant being run.
    stages:
        List of stages executed for this variant, or None if not specified.
    """
    print_benchmark(dataset_name, variant_name)

    if stages is None:
        print("Stages: default stages")
    else:
        stage_names = [_format_stage_name(stage) for stage in stages]
        print(f"Stages: {', '.join(stage_names)}")

    print("=" * 80 + "\n")
