import argparse
import sys
from pathlib import Path


def parse_args(stage_names: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clovertex pipeline runner")
    parser.add_argument("--start-at", choices=stage_names, help="Start from stage")
    parser.add_argument("--stop-at", choices=stage_names, help="Stop after stage")
    parser.add_argument("--list", action="store_true", help="List available stages")
    return parser.parse_args()


def run_pipeline(stages: list[tuple[str, callable]], start_at: str | None, stop_at: str | None):
    stage_names = [s[0] for s in stages]

    if start_at and stop_at:
        if stage_names.index(start_at) > stage_names.index(stop_at):
            raise ValueError("start-at must be before stop-at")

    active = False if start_at else True

    for name, func in stages:
        if start_at and name == start_at:
            active = True

        if not active:
            continue

        print(f"\n=== Running stage: {name} ===\n")
        func()

        if stop_at and name == stop_at:
            break


def main():
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    from pipeline.ingestion.ingest import run_ingestion
    from pipeline.cleaning.clean import run_cleaning
    from pipeline.transformation.transform import run_transformation
    from pipeline.analytics.analyze import run_analytics
    from pipeline.validation.validate import run_validation
    from pipeline.anomaly.detect import run_detection
    from pipeline.visualization.plots import run_visualization
    from pipeline.validation.manifest import run_manifests

    stages = [
        ("ingestion", run_ingestion),
        ("cleaning", run_cleaning),
        ("transformation", run_transformation),
        ("analytics", run_analytics),
        ("validation", run_validation),
        ("anomaly", run_detection),
        ("visualization", run_visualization),
        ("manifest", run_manifests),
    ]

    args = parse_args([s[0] for s in stages])

    if args.list:
        print("Available stages:")
        for name, _ in stages:
            print(f"- {name}")
        return

    try:
        run_pipeline(stages, args.start_at, args.stop_at)
    except Exception as exc:
        print(f"\nPipeline failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
