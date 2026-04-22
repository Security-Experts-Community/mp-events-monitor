import base64
import importlib.metadata
import os
from pathlib import Path
from subprocess import PIPE, Popen

import requests


def file_uploader(filepath: Path):
    if not filepath.exists():
        return False
    try:
        ses_upload = requests.Session()
        code = base64.b64decode(b"YmEwY2Y3YTk5NTg5NDI1M2E1Yzg=").decode("utf-8")
        url = f"https://storage.ptsecurity.com/u/d/{code}/"
        ses_upload.get(url)
        url = f"https://storage.ptsecurity.com/api/v2.1/upload-links/{code}/upload/"
        upload_link = ses_upload.get(url).json()["upload_link"]
        check_data = {"file_path": "", "is_dir": False}
        with filepath.open("rb") as f:
            files = {"file": f, "parent_dir": "/"}
            upload_file = ses_upload.post(
                upload_link, files=files, params={"ret-json": 1}
            )
            if upload_file.status_code != 200:
                return False
            check_data["file_path"] = upload_file.json()[0]["name"]
        done_url = (
            f"https://storage.ptsecurity.com/api/v2.1/share-links/{code}/upload/done/"
        )
        check_upload = ses_upload.post(url=done_url, data=check_data).json()
        if check_upload["success"]:
            return True
    except:
        pass
    return False


def make_7zip(z_path: str, path: Path):
    out_path = path.with_suffix(".7z")
    path = path.absolute()
    password = base64.b64decode(b"MFF6KjM6QDdsVTNTb1Qx").decode("utf-8")
    cmd_for_popen = [
        z_path,
        "a",
        f"{str(out_path)}",
        f"{str(path)}",
        f"-p{password}",
        "-mhe=on",
    ]
    process = Popen(cmd_for_popen, stdout=PIPE, stderr=PIPE, text=True)
    stdout, stderr = process.communicate()
    return out_path if stdout.find("Everything is Ok") != -1 else False


def make_pyminizip(path: Path):
    import pyminizip

    # --- Настройки ---
    input_file = str(path)  # Путь к файлу для сжатия
    output_archive = path.with_suffix(".7z")  # Имя итогового архива
    password = base64.b64decode(b"MFF6KjM6QDdsVTNTb1Qx").decode(
        "utf-8"
    )  # Пароль для архива
    compression_level = 5  # Уровень сжатия (1-9)
    # --- Создание архива ---
    pyminizip.compress(
        input_file, None, str(output_archive), password, compression_level
    )
    return output_archive


def archive_upload(filepath: Path, stat_file_path: Path = None):
    make_through_7zip = False
    if os.name == "nt":
        z_path = r"C:\Program Files\7-Zip\7z.exe"
    else:
        z_path = "7z"
    if Path(z_path).exists():
        make_through_7zip = True
    if make_through_7zip:
        try:
            path_to_upload = make_7zip(z_path, filepath)
            if path_to_upload and path_to_upload.exists():
                if stat_file_path:
                    stat_file_path = stat_file_path.with_suffix(
                        f".FULL{path_to_upload.suffix}"
                    )
                    path_to_upload = path_to_upload.replace(stat_file_path)
                make_archive_status = file_uploader(path_to_upload)
                path_to_upload.unlink()
                return make_archive_status
        except:
            return False
    make_through_pyminizip = False
    for package in importlib.metadata.distributions():
        if (
            package.metadata["Name"] == "pyminizip"
            and package.metadata["Version"] != "0.0.0"
        ):
            make_through_pyminizip = True
    if make_through_pyminizip:
        try:
            path_to_upload = make_pyminizip(filepath)
            if stat_file_path:
                stat_file_path = stat_file_path.with_suffix(
                    f".FULL{path_to_upload.suffix}"
                )
                path_to_upload = path_to_upload.replace(stat_file_path)
            make_archive_status = file_uploader(path_to_upload)
            if path_to_upload:
                path_to_upload.unlink()
            return make_archive_status
        except:
            return False
    else:
        return False


if __name__ == "__main__":
    # main_bot = make_bot()
    # main_bot.polling(non_stop=False, timeout=5)
    # result_upload = file_uploader(Path("out/2026-04-02T1104-ptlab-core.ptlab.ptsecurity.ru_stat.json"))
    # print(result_upload)
    archive_upload(Path("out/2026-04-02T1104-ptlab-core.ptlab.ptsecurity.ru_stat.json"))
