import re
import requests
from typing import List, Tuple, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from file import File
from base_downloader import BaseDownloader


class TZWorksDownloader(BaseDownloader):
    FILE_EXT_RE = re.compile(r"\.(zip|exe|7z|gz)$", re.I)
    HASH_LINK_RE = re.compile(r"(md5|sha1|sha256)", re.I)

    def list_files(self) -> List[File]:
        self.logger.info(f"[TZWorks] Scanning {self.url}")

        response = requests.get(self.url, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        results: List[File] = []

        for a in soup.find_all("a"):
            href = a.get("href")
            if not href:
                continue

            # skip hash files
            if self.HASH_LINK_RE.search(href):
                continue

            # only downloadable binaries
            if not self.FILE_EXT_RE.search(href):
                continue

            download_url = urljoin(self.url, href)
            name, version = self._parse_name_version(href)

            results.append(
                File(
                    name=name,
                    version=version,
                    download_page=self.url,
                    download_url=download_url,
                    site=self.site,
                )
            )

        self.logger.info(f"[TZWorks] Found {len(results)} files")
        return results

    def resolve_file_download_link(self, file: File) -> str:
        # TZWorks links are direct
        return file.download_url

    def _parse_name_version(self, filename: str) -> Tuple[str, Optional[str]]:
        base = filename.split("/")[-1]
        base = re.sub(self.FILE_EXT_RE, "", base)

        if ".v." in base:
            name, ver = base.split(".v.", 1)
            return name, ver.split(".")[0]

        return base, None
