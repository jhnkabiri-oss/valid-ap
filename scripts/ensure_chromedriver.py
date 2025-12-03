#!/usr/bin/env python3
"""
Simple helper script used by CI to ensure a chromedriver binary is available
and copy it to `bin/` inside the repo (for packaging / PyInstaller). The script
supports installing the detected version and optionally additional versions via
CHROMEDRIVER_VERSIONS env var (comma separated).

This avoids shell heredoc confusion and is cross platform.
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

try:
    import chromedriver_autoinstaller
except Exception as e:
    print("chromedriver_autoinstaller not installed. Please add to requirements.txt", file=sys.stderr)
    raise


def get_exe_version(exe_path: Path):
    try:
        out = subprocess.check_output([str(exe_path), "--version"], stderr=subprocess.STDOUT).decode().strip()
        # Example: "ChromeDriver 117.0.5938.62" / "chromedriver 117.0.5938.62"
        import re
        m = re.search(r"(\d+\.\d+\.\d+\.\d+|\d+\.\d+\.\d+)", out)
        if m:
            return m.group(1)
    except Exception as e:
        # Not necessarily an executable we can run (permissions) -> ignore
        pass
    return None


def install_version(v=None):
    try:
        if v:
            p = chromedriver_autoinstaller.install(v)
        else:
            p = chromedriver_autoinstaller.install()
        if not p:
            return None
        return Path(p)
    except Exception as e:
        print(f"Failed to install chromedriver version {v or 'auto'}: {e}", file=sys.stderr)
        return None


def main():
    outdir = Path.cwd() / "bin"
    outdir.mkdir(parents=True, exist_ok=True)

    versions_env = os.environ.get("CHROMEDRIVER_VERSIONS")
    if versions_env:
        versions = [v.strip() for v in versions_env.split(",") if v.strip()]
    else:
        versions = [None]

    installed = []
    for ver in versions:
        p = install_version(ver)
        if not p:
            print(f"No chromedriver for version {ver} was installed", file=sys.stderr)
            continue

        # Detect version of the downloaded chromedriver and copy with a version-suffix filename
        ver_str = get_exe_version(p) or (ver or "unknown")
        if os.name == 'nt':
            dest_name = f"chromedriver_{ver_str}.exe"
        else:
            dest_name = f"chromedriver_{ver_str}"

        dest = outdir / dest_name
        shutil.copy(p, dest)
        try:
            dest.chmod(0o755)
        except Exception:
            pass

        print(f"Copied chromedriver {p} -> {dest}")
        installed.append(str(dest))

    if not installed:
        print("ERROR: No chromedriver was installed or copied", file=sys.stderr)
        return 2

    # Also create a generic 'chromedriver.exe' or 'chromedriver' for --add-binary if needed
    preferred = installed[0]
    generic = outdir / ("chromedriver.exe" if os.name == 'nt' else "chromedriver")
    shutil.copy(Path(preferred), generic)
    try:
        generic.chmod(0o755)
    except Exception:
        pass

    print("INSTALLED_CHROMEDRIVERS=" + ",".join(installed))
    return 0


if __name__ == '__main__':
    sys.exit(main())
