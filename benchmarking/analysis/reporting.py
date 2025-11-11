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


def print_stages_benchmark_header(
    dataset_name: str, variant_name: str, enabled_stages: list | None
) -> None:
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
    print_benchmark(dataset_name, variant_name)

    if enabled_stages is None:
        print("Techniques: Exact matching only (always-on stages)")
    else:
        stage_names = [
            s.value if hasattr(s, "value") else str(s) for s in enabled_stages
        ]
        techniques = ["Exact matching (always-on)"] + stage_names
        print(f"Techniques: {', '.join(techniques)}")

    print("=" * 80 + "\n")
