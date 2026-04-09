import asyncio
import os
import re
import shutil
import aiohttp

from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

def what_os():
    if os.name == "nt":
        return "Windows"
    elif os.name == "posix":
        return "Posix"
    else:
        print("Unsupported operating system.")
        exit(1)

def compress(folder):
    folder = os.path.abspath(folder)
    folder_name = os.path.basename(os.path.normpath(folder))
    base_name = os.path.join(os.path.dirname(folder), folder_name)
    if what_os() == "Windows":
        shutil.make_archive(base_name, "zip", folder)
    else:
        shutil.make_archive(base_name, "gztar", folder)

class Scrape:
    SITES = []
    with open("sites.txt", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                SITES.append(line)

    def __init__(
        self,
        output_folder="88x31",
        max_per_site=None,
        timeout=8,
        progress_every=50,
        max_consecutive_failures=60,
        max_workers=16,
    ):
        self.output_folder = output_folder
        self.max_per_site = max_per_site
        self.timeout = timeout
        self.progress_every = progress_every
        self.max_consecutive_failures = max_consecutive_failures
        self.max_workers = max_workers

    def _fetch_text(self, url):
        request = Request(url, headers={"User-Agent": "Mozilla/5.0 88x31-Button-Scraper"})
        with urlopen(request, timeout=self.timeout) as response:
            return response.read().decode("utf-8", errors="ignore")

    def _extract_image_urls(self, html, base_url):
        image_urls = []
        for img_tag in re.findall(r"<img\b[^>]*>", html, flags=re.IGNORECASE):
            src_match = re.search(r'src=["\']([^"\']+)["\']', img_tag, flags=re.IGNORECASE)
            if not src_match:
                continue

            src = src_match.group(1).strip()
            width_match = re.search(r'width=["\']?(\d+)["\']?', img_tag, flags=re.IGNORECASE)
            height_match = re.search(r'height=["\']?(\d+)["\']?', img_tag, flags=re.IGNORECASE)

            looks_like_button = (
                "88x31" in src.lower()
                or "button" in src.lower()
                or (width_match and height_match and width_match.group(1) == "88" and height_match.group(1) == "31")
            )
            if not looks_like_button:
                continue

            full_url = urljoin(base_url, src)
            parsed = urlparse(full_url)
            if parsed.scheme not in {"http", "https"}:
                continue

            image_urls.append(full_url)

        return list(dict.fromkeys(image_urls))

    def _extract_page_urls(self, html, base_url, pattern):
        page_urls = []
        for href in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
            if not re.search(pattern, href, flags=re.IGNORECASE):
                continue

            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)
            if parsed.scheme in {"http", "https"}:
                page_urls.append(full_url)

        return list(dict.fromkeys(page_urls))

    def _resolve_tumblr_collection_pages(self, site_url):
        tumblr_html = self._fetch_text(site_url)
        source_match = re.search(
            r'https://capstasher\.neocities\.org/88x31collection-page\d+\.html',
            tumblr_html,
            flags=re.IGNORECASE,
        )
        if not source_match:
            return [site_url]

        first_page = source_match.group(0)
        pages_to_visit = [first_page]
        resolved_pages = []
        seen_pages = set()

        while pages_to_visit:
            page_url = pages_to_visit.pop(0)
            if page_url in seen_pages:
                continue

            seen_pages.add(page_url)
            resolved_pages.append(page_url)

            try:
                page_html = self._fetch_text(page_url)
            except Exception:
                continue

            for discovered_url in self._extract_page_urls(
                page_html,
                page_url,
                r'88x31collection-page\d+\.html',
            ):
                if discovered_url not in seen_pages:
                    pages_to_visit.append(discovered_url)

        return resolved_pages

    def _resolve_site_pages(self, site_url):
        if "tumblr.com/capstasher-development" in site_url:
            return self._resolve_tumblr_collection_pages(site_url)
        return [site_url]

    def _safe_name_from_url(self, url):
        host = urlparse(url).netloc.lower().replace(":", "_")
        return host.replace(".", "_")

    def _build_destination_paths(self, image_urls, destination_folder):
        destination_paths = []
        used_paths = set()

        for image_url in image_urls:
            parsed = urlparse(image_url)
            filename = os.path.basename(parsed.path) or "image"
            if "." not in filename:
                filename = f"{filename}.gif"
            filename = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)

            destination_path = os.path.join(destination_folder, filename)
            stem, ext = os.path.splitext(filename)
            suffix = 1
            while destination_path in used_paths or os.path.exists(destination_path):
                destination_path = os.path.join(destination_folder, f"{stem}_{suffix}{ext}")
                suffix += 1

            used_paths.add(destination_path)
            destination_paths.append(destination_path)

        return destination_paths

    def _download_image(self, image_url, destination_path):
        raise NotImplementedError("Use async download methods instead.")

    async def _write_file(self, destination_path, data):
        await asyncio.to_thread(self._write_file_sync, destination_path, data)

    def _write_file_sync(self, destination_path, data):
        with open(destination_path, "wb") as f:
            f.write(data)

    async def _download_image_async(self, session, image_url, destination_path):
        parsed = urlparse(image_url)
        if parsed.scheme not in {"http", "https"}:
            return False

        async with session.get(image_url, allow_redirects=True) as response:
            if response.status >= 400:
                return False

            content_type = response.headers.get("Content-Type", "")
            if not content_type.startswith("image/"):
                return False

            data = await response.read()

        await self._write_file(destination_path, data)
        return True

    async def _download_site_images_async(self, session, image_urls, site_name, site_output):
        downloaded_for_site = 0
        attempted_for_site = 0
        consecutive_failures = 0
        total_to_attempt = len(image_urls)
        destination_paths = self._build_destination_paths(image_urls, site_output)
        jobs = asyncio.Queue()
        state_lock = asyncio.Lock()
        stop_event = asyncio.Event()

        for image_url, destination_path in zip(image_urls, destination_paths):
            jobs.put_nowait((image_url, destination_path))

        async def worker():
            nonlocal downloaded_for_site, attempted_for_site, consecutive_failures
            while not stop_event.is_set():
                try:
                    image_url, destination_path = jobs.get_nowait()
                except asyncio.QueueEmpty:
                    return

                try:
                    success = await self._download_image_async(session, image_url, destination_path)
                except Exception:
                    success = False

                async with state_lock:
                    attempted_for_site += 1
                    if success:
                        downloaded_for_site += 1
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1

                    if attempted_for_site % self.progress_every == 0 or attempted_for_site == total_to_attempt:
                        print(
                            f"  Progress {site_name}: attempted {attempted_for_site}/{total_to_attempt}, "
                            f"downloaded {downloaded_for_site}"
                        )

                    if consecutive_failures >= self.max_consecutive_failures:
                        print(
                            f"  Stopping early for {site_name}: {consecutive_failures} consecutive failures "
                            f"(likely throttling/timeouts)."
                        )
                        stop_event.set()
                jobs.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(self.max_workers)]
        await asyncio.gather(*workers)

        return downloaded_for_site

    async def _run_async(self):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, self.output_folder)

        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path, exist_ok=True)

        total_downloaded = 0

        timeout = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(limit=self.max_workers, ttl_dns_cache=300)
        headers = {"User-Agent": "Mozilla/5.0 88x31-Button-Scraper"}

        async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
            for site in self.SITES:
                site_name = self._safe_name_from_url(site)
                site_output = os.path.join(output_path)

                print(f"Scraping {site}...")
                try:
                    image_urls = []
                    for page_url in self._resolve_site_pages(site):
                        html = self._fetch_text(page_url)
                        image_urls.extend(self._extract_image_urls(html, page_url))
                    image_urls = list(dict.fromkeys(image_urls))
                except Exception as exc:
                    print(f"Skipping {site}: {exc}")
                    continue

                site_limit = len(image_urls) if self.max_per_site is None else min(len(image_urls), self.max_per_site)
                selected_urls = image_urls[:site_limit]

                downloaded_for_site = await self._download_site_images_async(session, selected_urls, site_name, site_output)
                total_downloaded += downloaded_for_site

                print(f"Downloaded {downloaded_for_site} files from {site}.")

        if total_downloaded == 0:
            print("No images were downloaded; skipping compression and cleanup.")
            return

        compress(output_path)
        shutil.rmtree(output_path)
        print(f"Downloaded {total_downloaded} files total.")
        print(f"Compressed and removed original folder: {output_path}")

    def run(self):
        asyncio.run(self._run_async())

if __name__ == "__main__":
    print(f"Running on {what_os()}.")
    Scrape().run()