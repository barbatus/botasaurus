from typing import List

from .links import (
    _Base,
    apply_filters_maps_sorts_randomize,
    extract_link_upto_nth_segment,
)
from .list_utils import flatten
from .output import write_json
from .request_decorator import request
from .sitemap_parser_utils import (
    clean_robots_txt_url,
    clean_sitemap_url,
    extract_sitemaps,
    fix_bad_sitemap_response,
    fix_gzip_response,
    is_empty_path,
    parse_sitemaps_from_robots_txt,
    split_into_links_and_sitemaps,
    wrap_in_sitemap,
)

default_request_options = {
    "raise_exception": True,
    "create_error_logs": False,
    "close_on_crash": True,
    "output": None,
    "max_retry": 3,
}


def fetch_content(req, url: str):
    """Fetch content from a URL, handling gzip if necessary."""
    response = req.get(url, timeout=300)
    return fix_gzip_response(url, response)


class Sitemap(_Base):
    def __init__(self, urls: list[str], cache=True, proxy=None, parallel=40):
        self.cache = cache
        self.proxy = proxy
        self.parallel = parallel
        self.urls = urls if isinstance(urls, list) else [urls]

    def links(self) -> List[str]:
        request_options = self._create_request_options()

        urls = self._get_urls(request_options, self.urls)
        result = apply_filters_maps_sorts_randomize(
            urls,
            self._filters.get(0, []),
            self._extractors,
            self._sort_links,
            self._randomize_links,
        )
        return result

    def sitemaps(self) -> "Sitemap":
        request_options = self._create_request_options()

        self.urls = self._get_sitemaps_from_robots(request_options, self.urls)
        self.urls = self._get_sitemaps_urls(request_options, self.urls)

        return self

    def write_links(self, filename: str):
        results = self.links()
        write_json(results, filename)
        return results

    def write_sitemaps(self, filename: str):
        results = self.sitemaps()
        write_json(results, filename)
        return results

    def _create_request_options(self):
        options = {
            **default_request_options,
            "parallel": self.parallel,
            "cache": self.cache,
        }
        options["proxy"] = self.proxy
        return options

    def _get_sitemaps_urls(self, request_options, urls):
        visited = set()

        @request(**request_options)
        def sitemap(req, data):
            nonlocal visited

            url = data.get("url")
            if url in visited:
                return []

            visited.add(url)
            print(f"Visiting sitemap {url}")
            content = fix_bad_sitemap_response(fetch_content(req, url))
            if not content:
                return []

            locs = extract_sitemaps(content)
            level = data.get("level", 0)
            result = apply_filters_maps_sorts_randomize(
                locs,
                self._filters.get(level, []),
            )
            child_sitemaps = (
                sitemap(
                    wrap_in_sitemap(result, level=level + 1),
                    return_dont_cache_as_is=True,
                )
                if result
                else []
            )
            return ([url] + flatten(child_sitemaps)) if result else [url]

        return flatten(sitemap(wrap_in_sitemap(urls, level=1)))

    def _get_sitemaps_from_robots(self, request_options, urls):
        visited = set()

        @request(**request_options)
        def sitemap(req, url):
            nonlocal visited

            if url in visited:
                return []

            visited.add(url)
            content = fetch_content(req, url)

            if not content:
                return []

            result = parse_sitemaps_from_robots_txt(
                extract_link_upto_nth_segment(0, url), content
            )
            if not result:
                sm_url = clean_sitemap_url(url)
                content = fetch_content(req, sm_url)
                return [sm_url] if content else []
            result = apply_filters_maps_sorts_randomize(
                result,
                self._filters.get(0, []),
            )
            return result

        return flatten(
            sitemap(clean_robots_txt_url(url)) if is_empty_path(url) else url
            for url in urls
        )

    def _get_urls(self, request_options, urls):
        visited = set()

        @request(**request_options)
        def sitemap(req, url):
            nonlocal visited  # Reference the global visited set

            if url in visited:
                return []

            visited.add(url)
            print(f"Extracting links from {url}")
            content = fix_bad_sitemap_response(fetch_content(req, url))

            links, locs = split_into_links_and_sitemaps(content)

            return links

        return flatten(
            sitemap(
                urls,
            )
        )
