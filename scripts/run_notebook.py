"""Execute the real notebook with this interpreter and an isolated kernel spec."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile


def main() -> None:
    import nbformat
    from nbclient import NotebookClient
    from traitlets.config import Config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/Streaming_Lab_HE.executed.ipynb"))
    parser.add_argument("--transport", choices=("tcp", "ipc"), default="tcp",
                        help="Use ipc for a local POSIX environment without loopback networking")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    source = root / "Streaming_Lab_HE.ipynb"
    notebook = nbformat.read(source, as_version=4)
    nbformat.validate(notebook)
    previous_path = os.environ.get("JUPYTER_PATH")
    with tempfile.TemporaryDirectory(prefix="streaming-kernel-") as temp:
        share = Path(temp) / "share" / "jupyter"
        kernel = share / "kernels" / "streaming-course-test"
        kernel.mkdir(parents=True)
        (kernel / "kernel.json").write_text(json.dumps({
            "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
            "display_name": "Streaming course test",
            "language": "python",
        }), encoding="utf-8")
        os.environ["JUPYTER_PATH"] = str(share) + (os.pathsep + previous_path if previous_path else "")
        config = Config()
        if args.transport == "ipc":
            if os.name == "nt":
                raise ValueError("Use the default TCP transport on Windows")
            config.KernelManager.transport = "ipc"
            config.KernelManager.ip = str(Path(temp) / "channel")
        try:
            client = NotebookClient(
                notebook,
                timeout=180,
                kernel_name="streaming-course-test",
                resources={"metadata": {"path": str(root)}},
                allow_errors=False,
                record_timing=True,
                config=config,
            )
            client.execute()
        finally:
            if previous_path is None:
                os.environ.pop("JUPYTER_PATH", None)
            else:
                os.environ["JUPYTER_PATH"] = previous_path
    output = args.output if args.output.is_absolute() else root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    nbformat.write(notebook, output)
    code_cells = [cell for cell in notebook.cells if cell.cell_type == "code"]
    errors = [out for cell in code_cells for out in cell.get("outputs", []) if out.output_type == "error"]
    assert not errors
    summary = {
        "python": sys.version.split()[0],
        "kernel_transport": args.transport,
        "code_cells": len(code_cells),
        "executed_cells": sum(cell.execution_count is not None for cell in code_cells),
        "errors": len(errors),
        "executed_notebook": str(output.relative_to(root)) if output.is_relative_to(root) else str(output),
    }
    (output.parent / "notebook_execution.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
