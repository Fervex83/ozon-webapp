import json
import os
import platform
import random
import re
import shutil
import subprocess
import tempfile
import time
from urllib.parse import quote_plus
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException, SessionNotCreatedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

DEFAULT_PAGE_TIMEOUT_SEC = int(os.getenv("OZON_PAGE_TIMEOUT", "90"))
DEFAULT_GET_RETRIES = int(os.getenv("OZON_GET_RETRIES", "3"))
DEFAULT_LABEL_WAIT_SEC = int(os.getenv("OZON_LABEL_WAIT", "10"))
DEFAULT_HEADLESS = os.getenv("OZON_HEADLESS", "0") == "1"
DEBUG_MODE = os.getenv("OZON_DEBUG", "0") == "1"
CLICK_LABEL = os.getenv("OZON_CLICK_LABEL", "0") == "1"
USER_DATA_DIR = Path(os.getenv("OZON_USER_DATA_DIR", "ozon_profile_web"))
CHROMEDRIVER_LOG = os.getenv("OZON_CHROMEDRIVER_LOG", "chromedriver.log")
SELLER_ALIASES_PATH = Path(
    os.getenv(
        "OZON_SELLER_ALIASES",
        str(Path(__file__).resolve().parent / "data" / "seller_aliases.json"),
    )
)
DEFAULT_SEARCH_SCROLLS = int(os.getenv("OZON_SEARCH_SCROLLS", "2"))
DEFAULT_SEARCH_MAX_PAGES = int(os.getenv("OZON_SEARCH_MAX_PAGES", "0"))
DEFAULT_SEARCH_LOAD_WAIT_SEC = float(os.getenv("OZON_SEARCH_LOAD_WAIT", "1.0"))
DEFAULT_SEARCH_SCROLL_WAIT_SEC = float(os.getenv("OZON_SEARCH_SCROLL_WAIT", "0.7"))
DEFAULT_SEARCH_STABLE_HITS = int(os.getenv("OZON_SEARCH_STABLE_HITS", "1"))
DEFAULT_SEARCH_STABLE_PAUSE_SEC = float(os.getenv("OZON_SEARCH_STABLE_PAUSE", "0.3"))


@dataclass
class CheckResult:
    url: str
    ok: bool
    has_label: bool
    seller_ok: Optional[bool]
    seller_name: Optional[str]
    label_text: str
    error: Optional[str]


def normalize_text(s: str) -> str:
    s = s.lower()
    s = s.replace("ё", "е")
    s = s.replace("🎁", "подарок")
    s = re.sub(r"[^\w]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


_SELLER_ALIASES_CACHE: Optional[dict[str, list[str]]] = None


def load_seller_aliases() -> dict[str, list[str]]:
    global _SELLER_ALIASES_CACHE
    if _SELLER_ALIASES_CACHE is not None:
        return _SELLER_ALIASES_CACHE
    try:
        if not SELLER_ALIASES_PATH.exists():
            _SELLER_ALIASES_CACHE = {}
            return _SELLER_ALIASES_CACHE
        raw = json.loads(SELLER_ALIASES_PATH.read_text(encoding="utf-8") or "{}")
        aliases: dict[str, list[str]] = {}
        if isinstance(raw, dict):
            for key, values in raw.items():
                key_norm = normalize_text(str(key))
                if not key_norm:
                    continue
                if isinstance(values, list):
                    vals = [normalize_text(str(v)) for v in values if normalize_text(str(v))]
                else:
                    vals = [normalize_text(str(values))] if normalize_text(str(values)) else []
                if vals:
                    aliases[key_norm] = list(dict.fromkeys(vals))
        _SELLER_ALIASES_CACHE = aliases
    except Exception:
        _SELLER_ALIASES_CACHE = {}
    return _SELLER_ALIASES_CACHE




def is_label_candidate(norm_text: str, has_icon: bool = False) -> bool:
    if not norm_text:
        return False
    has_sim = "sim" in norm_text
    has_brand = "tecno" in norm_text or "карта" in norm_text
    has_gift = "подарок" in norm_text or "gift" in norm_text
    if has_icon:
        return has_sim and has_brand
    return has_sim and has_brand and has_gift


def extract_label_link(driver: webdriver.Chrome) -> Optional[str]:
    try:
        href = driver.execute_script(
            """
            const isHit = (t) => {
              if (!t) return false;
              const s = String(t).toLowerCase();
              return s.includes("sim") && s.includes("tecno") && (s.includes("🎁") || s.includes("подар"));
            };
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
            while (walker.nextNode()) {
              const el = walker.currentNode;
              const text = el.textContent || "";
              if (!isHit(text)) continue;
              const link = el.closest("a");
              if (link && link.href) return link.href;
            }
            return "";
            """
        )
        return href or None
    except Exception:
        return None


def click_label_by_text(driver: webdriver.Chrome) -> bool:
    try:
        clicked = driver.execute_script(
            """
            const isHit = (t) => {
              if (!t) return false;
              const s = String(t).toLowerCase();
              return s.includes("sim") && s.includes("tecno") && (s.includes("🎁") || s.includes("подар"));
            };
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
            while (walker.nextNode()) {
              const el = walker.currentNode;
              const text = el.textContent || "";
              if (!isHit(text)) continue;
              const target =
                el.closest("a") ||
                el.closest("button") ||
                el.closest("[role='button']") ||
                el;
              target.scrollIntoView({ block: "center" });
              target.click();
              return true;
            }
            return false;
            """
        )
        return bool(clicked)
    except Exception:
        return False


def filter_label_chunks(chunks: list[str], has_icon: bool = False) -> list[str]:
    cleaned = []
    seen = set()
    for raw in chunks:
        text = (str(raw) if raw is not None else "").strip()
        if not text:
            continue
        norm = normalize_text(text)
        if "перейти к описанию" in norm:
            continue
        if not is_label_candidate(norm, has_icon=has_icon):
            continue
        if norm in seen:
            continue
        seen.add(norm)
        cleaned.append(text)
    return cleaned


def find_chrome_binary() -> Optional[str]:
    system = platform.system()
    if system == "Windows":
        candidates = [
            r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            r"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
        ]
        for path in candidates:
            if Path(path).exists():
                return path
        return None
    if system == "Darwin":
        path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        return path if Path(path).exists() else None
    for name in ["google-chrome", "chrome", "chromium", "chromium-browser"]:
        found = shutil.which(name)
        if found:
            return found
    return None


def create_driver(clean_profile: bool = False) -> webdriver.Chrome:
    options = Options()
    temp_profile = None
    if clean_profile:
        temp_profile = Path(tempfile.mkdtemp(prefix="ozon_profile_"))
        options.add_argument(f"--user-data-dir={temp_profile.resolve()}")
    else:
        options.add_argument(f"--user-data-dir={USER_DATA_DIR.resolve()}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    options.page_load_strategy = "eager"

    if DEFAULT_HEADLESS:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1400,900")

    chrome_binary = find_chrome_binary()
    if chrome_binary:
        options.binary_location = chrome_binary

    driver_path = ChromeDriverManager().install()
    try:
        result = subprocess.run(
            [driver_path, "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout.strip():
            print(f"ChromeDriver: {result.stdout.strip()}")
        elif result.stderr.strip():
            print(f"ChromeDriver: {result.stderr.strip()}")
        print(f"ChromeDriver path: {driver_path}")
    except Exception:
        pass

    service = Service(driver_path, log_path=CHROMEDRIVER_LOG)
    try:
        driver = webdriver.Chrome(service=service, options=options)
    except SessionNotCreatedException as e:
        if "internal JSON template" in str(e):
            temp_profile = Path(tempfile.mkdtemp(prefix="ozon_tmp_profile_"))
            options = Options()
            options.add_argument(f"--user-data-dir={temp_profile.resolve()}")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--no-first-run")
            options.add_argument("--no-default-browser-check")
            options.page_load_strategy = "eager"
            if DEFAULT_HEADLESS:
                options.add_argument("--headless=new")
                options.add_argument("--window-size=1400,900")
            if chrome_binary:
                options.binary_location = chrome_binary
            driver = webdriver.Chrome(service=service, options=options)
        else:
            raise

    driver.set_page_load_timeout(DEFAULT_PAGE_TIMEOUT_SEC)
    driver._ozon_temp_profile = temp_profile
    return driver


def safe_get(driver: webdriver.Chrome, url: str, retries: int = DEFAULT_GET_RETRIES) -> bool:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            driver.get(url)
            return True
        except TimeoutException as e:
            last_err = e
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            time.sleep(2 + attempt)
        except WebDriverException as e:
            last_err = e
            time.sleep(2 + attempt)
    if last_err:
        print(f"[GET FAILED] {url} -> {last_err}")
    return False


def build_search_url(query: str, page: int) -> str:
    encoded = quote_plus(query)
    return f"https://www.ozon.ru/search/?text={encoded}&page={page}"




def normalize_product_url(url: str) -> Optional[str]:
    if not url:
        return None
    if "/product/" not in url:
        return None
    if url.startswith("/"):
        url = f"https://www.ozon.ru{url}"
    if "ozon.ru" not in url:
        return None
    clean = url.split("?")[0].rstrip("/")
    return clean


def collect_search_urls(
    query: str,
    seller_filter: Optional[str] = None,
    max_pages: int = DEFAULT_SEARCH_MAX_PAGES,
    scrolls: Optional[int] = None,
    load_wait_sec: Optional[float] = None,
    scroll_wait_sec: Optional[float] = None,
    stable_hits: Optional[int] = None,
    stable_pause_sec: Optional[float] = None,
    clean_profile: bool = False,
    progress_cb: Optional[Callable[[list[str]], None]] = None,
    raw_cb: Optional[Callable[[list[str]], None]] = None,
    seller_progress_cb: Optional[Callable[[int, int, int], None]] = None,
    eta_cb: Optional[Callable[[str, float], None]] = None,
    match_test_cb: Optional[Callable[[webdriver.Chrome, str], Optional[CheckResult]]] = None,
    match_result_cb: Optional[Callable[[CheckResult], None]] = None,
    phase_cb: Optional[Callable[[str], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> list[str]:
    driver = create_driver(clean_profile=clean_profile)

    urls: list[str] = []
    seen: set[str] = set()
    page = 1
    page_times: list[float] = []

    def grab_all_links_from_results() -> list[str]:
        """
        Берём ВСЕ product-ссылки из основного грида результатов (без рекомендаций ниже).
        Не фильтруем по viewport — иначе результат будет плавать.
        """
        try:
            return driver.execute_script(
                """
                const container = document.querySelector("#contentScrollPaginator") || document;

                // Пытаемся отрезать блок "Возможно, вам понравится" (рекомендации)
                const heading = Array.from(container.querySelectorAll("h2, h3, h4, span, div"))
                  .find((el) =>
                    (el.textContent || "").trim().toLowerCase().includes("возможно, вам понравится")
                  );

                const grids = container.querySelectorAll(
                  "[data-widget='tileGridDesktop'], [data-widget*='tileGrid']"
                );

                let root = null;
                if (grids.length) {
                  if (heading) {
                    const headTop = heading.getBoundingClientRect().top + window.scrollY;
                    root = Array.from(grids).find((grid) => {
                      const gridTop = grid.getBoundingClientRect().top + window.scrollY;
                      return gridTop < headTop;
                    });
                  }
                  root = root || grids[0];
                }
                if (!root) root = container;

                const out = [];
                const links = root.querySelectorAll("a[href*='/product/']");
                links.forEach((link) => {
                  const href = link.getAttribute("href");
                  if (href) out.push(href);
                });
                return out;
                """
            )
        except Exception:
            return []

    def collect_new(hrefs: list[str]) -> int:
        new_count = 0
        for href in hrefs or []:
            norm = normalize_product_url(str(href))
            if not norm or norm in seen:
                continue
            seen.add(norm)
            urls.append(norm)
            new_count += 1
            if progress_cb:
                progress_cb(list(urls))
        return new_count

    try:
        if phase_cb:
            phase_cb("search")
        while True:
            if cancel_check and cancel_check():
                break

            target = build_search_url(query, page)
            page_started = time.time()
            if not safe_get(driver, target):
                break

            wait_after_load = (
                DEFAULT_SEARCH_LOAD_WAIT_SEC if load_wait_sec is None else float(load_wait_sec)
            )
            time.sleep(random.uniform(wait_after_load, wait_after_load + 0.6))

            total_scrolls = DEFAULT_SEARCH_SCROLLS if scrolls is None else max(1, int(scrolls))
            wait_after_scroll = (
                DEFAULT_SEARCH_SCROLL_WAIT_SEC if scroll_wait_sec is None else float(scroll_wait_sec)
            )

            page_new_count = 0

            # 1) сбор сразу после загрузки
            page_new_count += collect_new(grab_all_links_from_results())

            # 2) скроллы + сбор после каждого скролла (без viewport-фильтра)
            for _ in range(total_scrolls):
                if cancel_check and cancel_check():
                    break
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                except Exception:
                    pass
                time.sleep(wait_after_scroll)
                page_new_count += collect_new(grab_all_links_from_results())

            # 3) стабилизация: ждём, пока количество product-ссылок перестанет расти
            target_hits = DEFAULT_SEARCH_STABLE_HITS if stable_hits is None else max(0, int(stable_hits))
            pause_sec = (
                DEFAULT_SEARCH_STABLE_PAUSE_SEC if stable_pause_sec is None else float(stable_pause_sec)
            )
            if target_hits:
                stable_hits_count = 0
                last_count = -1
                while stable_hits_count < target_hits:
                    if cancel_check and cancel_check():
                        break
                    try:
                        count = len(grab_all_links_from_results())
                    except Exception:
                        count = -1
                    if count == last_count and count > 0:
                        stable_hits_count += 1
                    else:
                        stable_hits_count = 0
                    last_count = count
                    time.sleep(pause_sec)

                # после стабилизации — ещё раз финальный сбор
                page_new_count += collect_new(grab_all_links_from_results())

            # если совсем ничего нового — заканчиваем
            if page_new_count == 0:
                break

            if max_pages and page >= max_pages:
                break

            page += 1
            page_times.append(time.time() - page_started)
            if max_pages and eta_cb and page_times:
                avg_page = sum(page_times[-5:]) / len(page_times[-5:])
                remaining = max(0, max_pages - page)
                eta_cb("search", remaining * avg_page)

        # ----- seller_filter как у тебя было -----
        if seller_filter:
            if raw_cb:
                raw_cb(list(urls))
            if phase_cb:
                phase_cb("seller")
            filtered: list[str] = []
            total = len(urls)
            checked = 0
            for url in urls:
                if cancel_check and cancel_check():
                    break
                if not safe_get(driver, url):
                    continue
                time.sleep(random.uniform(0.1, 0.25))
                try:
                    body_text_raw = driver.find_element(By.TAG_NAME, "body").text
                except Exception:
                    body_text_raw = ""
                body_text = normalize_text(body_text_raw)
                page_source = driver.page_source or ""
                seller_name = extract_seller_name(driver)
                if not seller_name:
                    seller_name = extract_seller_from_source(page_source)
                if not seller_name:
                    seller_name = extract_seller_from_text(body_text_raw)
                seller_ok = is_ozon_seller(seller_name, body_text)
                checked += 1
                if seller_matches_filter(seller_filter, seller_name, seller_ok, body_text):
                    filtered.append(url)
                    if progress_cb:
                        progress_cb(list(filtered))
                    if match_test_cb and match_result_cb:
                        try:
                            res = match_test_cb(driver, url)
                        except Exception:
                            res = None
                        if res:
                            match_result_cb(res)
                if seller_progress_cb:
                    seller_progress_cb(checked, total, len(filtered))
            return filtered

        if raw_cb:
            raw_cb(list(urls))
        return urls

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def count_listing_cards(
    url: str,
    scroll_pause_sec: float = 1.2,
    stable_rounds: int = 3,
    max_scrolls: int = 80,
    clean_profile: bool = False,
) -> dict:
    """
    Считает количество карточек (product links) на листинге OZON.
    Логика:
    - открываем URL
    - скроллим вниз
    - после каждого скролла собираем ВСЕ ссылки /product/
    - останавливаемся, когда количество перестало расти stable_rounds раз подряд
      или когда достигли max_scrolls
    Возвращает: {"count": int, "urls": list[str]}
    """
    driver = create_driver(clean_profile=clean_profile)

    def grab_product_links() -> list[str]:
        try:
            hrefs = driver.execute_script(
                """
                // Ищем основные гриды с товарами
                const grids = document.querySelectorAll(
                  "[data-widget='tileGridDesktop'], [data-widget*='tileGrid']"
                );

                let root = null;
                if (grids && grids.length) root = grids[0];
                if (!root) root = document;

                const out = [];
                const links = root.querySelectorAll("a[href*='/product/']");
                links.forEach(a => {
                  const h = a.getAttribute("href");
                  if (h) out.push(h);
                });
                return out;
                """
            )
        except Exception:
            hrefs = []

        # Нормализация (чтобы не было дублей из-за query-параметров)
        norm = []
        seen = set()
        for h in hrefs or []:
            u = normalize_product_url(str(h))
            if u and u not in seen:
                seen.add(u)
                norm.append(u)
        return norm

    try:
        if not safe_get(driver, url):
            return {"count": 0, "urls": []}

        # дать странице прогрузиться
        time.sleep(random.uniform(2.0, 3.0))

        last_count = -1
        stable = 0
        urls: list[str] = []

        for _ in range(max_scrolls):
            # сбор
            urls = grab_product_links()
            curr = len(urls)

            if curr == last_count and curr > 0:
                stable += 1
            else:
                stable = 0

            last_count = curr

            if stable >= stable_rounds:
                break

            # скролл дальше
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception:
                pass

            time.sleep(random.uniform(scroll_pause_sec, scroll_pause_sec + 0.6))

        return {"count": len(urls), "urls": urls}

    finally:
        try:
            driver.quit()
        except Exception:
            pass



def collect_label_text(driver: webdriver.Chrome) -> str:
    try:
        WebDriverWait(driver, DEFAULT_LABEL_WAIT_SEC).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, "[data-widget='webMarketingLabels']")
        )
    except Exception:
        pass
    try:
        WebDriverWait(driver, DEFAULT_LABEL_WAIT_SEC).until(
            lambda d: d.find_elements(By.CSS_SELECTOR, ".b5_5_1-a5[title], .b5_5_1-a5")
        )
    except Exception:
        pass
    try:
        WebDriverWait(driver, DEFAULT_LABEL_WAIT_SEC).until(
            lambda d: d.execute_script(
                """
                const root = document.querySelectorAll("[data-widget='webMarketingLabels']");
                for (const node of root) {
                  if (node.innerText && node.innerText.trim()) return true;
                  const titled = node.querySelector("[title],[aria-label],img[alt]");
                  if (titled) return true;
                }
                return false;
                """
            )
        )
    except Exception:
        pass

    try:
        chunks = driver.execute_script(
            """
            const root = document.querySelectorAll("[data-widget='webMarketingLabels']");
            const out = [];
            root.forEach((node) => {
              if (node.innerText) out.push(node.innerText);
              node.querySelectorAll("[title],[aria-label],img[alt]").forEach((el) => {
                const t = el.getAttribute("title") || el.getAttribute("aria-label") || el.getAttribute("alt");
                if (t) out.push(t);
              });
            });
            const titled = document.querySelector(".b5_5_1-a5[title]");
            if (titled) out.push(titled.getAttribute("title"));
            document.querySelectorAll(".b5_5_1-a5").forEach((node) => {
              if (node.innerText) out.push(node.innerText);
              const t = node.getAttribute("title");
              if (t) out.push(t);
            });
            return out;
            """
        )
        has_icon = False
        try:
            has_icon = driver.execute_script(
                """
                const roots = document.querySelectorAll("[data-widget='webMarketingLabels']");
                for (const node of roots) {
                  if (node.querySelector("img,svg")) return true;
                }
                return false;
                """
            )
        except Exception:
            has_icon = False
        # DEBUG BLOCK (enable if needed)
        # if DEBUG_MODE:
        #     try:
        #         html_snippets = driver.execute_script(
        #             """
        #             return Array.from(document.querySelectorAll("[data-widget='webMarketingLabels']"))
        #               .map((n) => n.outerHTML)
        #               .slice(0, 2);
        #             """
        #         )
        #         print("[DEBUG] webMarketingLabels outerHTML:", html_snippets)
        #     except Exception as e:
        #         print("[DEBUG] webMarketingLabels outerHTML error:", e)
        if chunks:
            filtered = filter_label_chunks([str(x) for x in chunks if x], has_icon=has_icon)
            if filtered:
                combined = " ".join(filtered)
                if has_icon and "подарок" not in normalize_text(combined):
                    return f"{combined} 🎁"
                return combined
    except Exception:
        pass

    try:
        chunks = driver.execute_script(
            """
            const out = [];
            const isHit = (t) => {
              if (!t) return false;
              const s = String(t).toLowerCase();
              return s.includes("sim") && s.includes("tecno") && (s.includes("🎁") || s.includes("подар"));
            };
            const pushIfHit = (t) => {
              if (isHit(t)) out.push(t);
            };
            const walk = (node) => {
              if (!node) return;
              if (node.nodeType === Node.ELEMENT_NODE) {
                const el = node;
                if (!el.children || el.children.length === 0) {
                  pushIfHit(el.textContent);
                }
                pushIfHit(el.getAttribute && el.getAttribute("title"));
                pushIfHit(el.getAttribute && el.getAttribute("aria-label"));
                pushIfHit(el.getAttribute && el.getAttribute("alt"));
                if (el.shadowRoot) walk(el.shadowRoot);
              }
              if (node.childNodes) {
                node.childNodes.forEach(walk);
              }
            };
            walk(document.body);
            return out;
            """
        )
        if chunks:
            filtered = filter_label_chunks([str(x) for x in chunks if x])
            if filtered:
                return " ".join(filtered)
    except Exception:
        pass

    label_nodes = driver.find_elements(By.CSS_SELECTOR, "[data-widget='webMarketingLabels']")
    chunks = []
    for node in label_nodes:
        try:
            if node.text:
                chunks.append(node.text)
            for titled in node.find_elements(By.CSS_SELECTOR, "[title],[aria-label],img[alt]"):
                title_val = titled.get_attribute("title")
                if not title_val:
                    title_val = titled.get_attribute("aria-label")
                if not title_val:
                    title_val = titled.get_attribute("alt")
                if title_val:
                    chunks.append(title_val)
        except Exception:
            continue
    if not chunks:
        try:
            for node in driver.find_elements(By.CSS_SELECTOR, ".b5_5_1-a5"):
                title_val = node.get_attribute("title")
                if title_val:
                    chunks.append(title_val)
                if node.text:
                    chunks.append(node.text)
        except Exception:
            pass
    filtered = filter_label_chunks(chunks)
    return " ".join(filtered)


def extract_label_from_source(page_source: str) -> Optional[str]:
    if not page_source:
        return None
    candidates = []
    candidates += re.findall(
        r'class="[^"]*b5_5_1-a5[^"]*"[^>]*title="([^"]+)"', page_source
    )
    candidates += re.findall(
        r'class="[^"]*b5_5_1-a5[^"]*"[^>]*>([^<]+)</', page_source
    )
    for cand in candidates:
        cand_norm = normalize_text(cand)
        if is_label_candidate(cand_norm):
            return cand
    return None


def extract_label_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    return None


def check_current_page(driver: webdriver.Chrome, url: str) -> CheckResult:
    time.sleep(random.uniform(0.4, 0.8))
    try:
        driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.3));")
        time.sleep(0.2)
        driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.6));")
        time.sleep(0.2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.2)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.15)
    except Exception:
        pass

    label_text = collect_label_text(driver)
    has_label = label_present(label_text)

    try:
        body_text_raw = driver.find_element(By.TAG_NAME, "body").text
    except Exception:
        body_text_raw = ""
    body_text = normalize_text(body_text_raw)

    page_source = driver.page_source or ""

    if not has_label:
        label_from_source = extract_label_from_source(page_source)
        if label_from_source:
            label_text = label_from_source
            has_label = True
        else:
            label_from_body = extract_label_from_text(body_text_raw)
            if label_from_body:
                label_text = label_from_body
                has_label = True

    seller_name = extract_seller_name(driver)
    if not seller_name:
        seller_name = extract_seller_from_source(page_source)
    if not seller_name:
        seller_name = extract_seller_from_text(body_text_raw)

    if has_label and CLICK_LABEL:
        _clicked = click_label_by_text(driver)
        if _clicked:
            time.sleep(0.5)
    seller_ok = is_ozon_seller(seller_name, body_text)
    return CheckResult(
        url=url,
        ok=True,
        has_label=has_label,
        seller_ok=seller_ok,
        seller_name=seller_name,
        label_text=label_text,
        error=None,
    )




def extract_seller_from_source(page_source: str) -> Optional[str]:
    if not page_source:
        return None
    patterns = [
        r'"sellerName":"([^"]+)"',
        r'"merchantName":"([^"]+)"',
        r'"seller":"([^"]+)"',
        r'"companyName":"([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, page_source)
        if match:
            return match.group(1)
    return None


def extract_seller_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"\bozon express\b", text, flags=re.IGNORECASE)
    if match:
        return match.group(0)
    match = re.search(r"\bozon\b", text, flags=re.IGNORECASE)
    if match:
        return match.group(0)
    return None


def label_present(label_text: str) -> bool:
    norm = normalize_text(label_text or "")
    return is_label_candidate(norm, has_icon="🎁" in (label_text or ""))


def extract_seller_name(driver: webdriver.Chrome) -> Optional[str]:
    selectors = [
        "[data-widget='webProductSeller']",
        "[data-widget='webOutOfStockSeller']",
        "div[class*='b35_3_18-a9'] span[class*='b35_3_18-b6']",
        "span[class*='b35_3_18-b6']",
        "div[class*='pdp_a5m'] span[class*='b35_3_18-b6']",
    ]
    for selector in selectors:
        try:
            WebDriverWait(driver, DEFAULT_LABEL_WAIT_SEC).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, selector)
            )
        except Exception:
            continue
    for selector in selectors:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elems:
                text = (elem.text or "").strip()
                if text and normalize_text(text) != "перейти":
                    return text
        except Exception:
            continue
    for selector in selectors:
        try:
            text = driver.execute_script(
                """
                const el = document.querySelector(arguments[0]);
                return el ? (el.textContent || "").trim() : "";
                """,
                selector,
            )
            # DEBUG BLOCK (enable if needed)
            # if DEBUG_MODE:
            #     try:
            #         html = driver.execute_script(
            #             """
            #             const el = document.querySelector(arguments[0]);
            #             return el ? el.outerHTML : "";
            #             """,
            #             selector,
            #         )
            #         print("[DEBUG] seller selector:", selector)
            #         print("[DEBUG] seller textContent:", text)
            #         print("[DEBUG] seller outerHTML:", html)
            #     except Exception as e:
            #         print("[DEBUG] seller debug error:", e)
            if text and normalize_text(text) != "перейти":
                return text
        except Exception:
            continue
    return None


def is_ozon_seller(seller_name: Optional[str], body_text: str) -> Optional[bool]:
    if seller_name:
        seller_normalized = normalize_text(seller_name)
        return bool(re.search(r"\bozon\b", seller_normalized))

    if not body_text:
        return None
    seller_normalized = normalize_text(body_text)
    if re.search(r"\bozon\b", seller_normalized):
        return True
    if "продавец" in seller_normalized:
        return False
    return None


def _split_seller_filter(value: str) -> list[str]:
    if not value:
        return []
    parts = re.split(r"[,\n;]+", value)
    out: list[str] = []
    for part in parts:
        norm = normalize_text(part.strip())
        if norm:
            out.append(norm)
    return out


def expand_seller_aliases(values: list[str]) -> list[str]:
    if not values:
        return []
    expanded = set(values)
    aliases = load_seller_aliases()
    if aliases:
        for key in list(expanded):
            extra = aliases.get(key)
            if extra:
                expanded.update(extra)
    return list(expanded)






def seller_matches_filter(
    filter_value: str,
    seller_name: Optional[str],
    seller_ok: Optional[bool],
    body_text: str,
) -> bool:
    values = expand_seller_aliases(_split_seller_filter(filter_value or ""))
    if not values:
        return True
    if not seller_name:
        return False
    seller_norm = normalize_text(seller_name)
    # Строго по имени продавца, без совпадений в body_text.
    return any(seller_norm == val for val in values)


def check_url(url: str) -> CheckResult:
    driver = create_driver()
    try:
        ok = safe_get(driver, url)
        if not ok:
            return CheckResult(
                url=url,
                ok=False,
                has_label=False,
                seller_ok=None,
                seller_name=None,
                label_text="",
                error="Не удалось открыть страницу после повторов.",
            )

        time.sleep(random.uniform(0.5, 1.0))
        try:
            driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.3));")
            time.sleep(0.25)
            driver.execute_script("window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.6));")
            time.sleep(0.25)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.25)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.2)
        except Exception:
            pass

        label_text = collect_label_text(driver)
        has_label = label_present(label_text)

        try:
            body_text_raw = driver.find_element(By.TAG_NAME, "body").text
        except Exception:
            body_text_raw = ""
        body_text = normalize_text(body_text_raw)

        page_source = driver.page_source or ""
        # DEBUG BLOCK (enable if needed)
        # if DEBUG_MODE:
        #     try:
        #         print("[DEBUG] current_url:", driver.current_url)
        #         print("[DEBUG] title:", driver.title)
        #         Path("debug_page.html").write_text(page_source, encoding="utf-8")
        #         Path("debug_body.txt").write_text(body_text_raw, encoding="utf-8")
        #         driver.save_screenshot("debug_page.png")
        #         print("[DEBUG] wrote debug_page.html, debug_body.txt, debug_page.png")
        #     except Exception as e:
        #         print("[DEBUG] debug dump error:", e)

        if not has_label:
            label_from_source = extract_label_from_source(page_source)
            if label_from_source:
                label_text = label_from_source
                has_label = True
            else:
                label_from_body = extract_label_from_text(body_text_raw)
                if label_from_body:
                    label_text = label_from_body
                    has_label = True

        seller_name = extract_seller_name(driver)
        if not seller_name:
            seller_name = extract_seller_from_source(page_source)
        if not seller_name:
            seller_name = extract_seller_from_text(body_text_raw)

        if has_label and CLICK_LABEL:
            _clicked = click_label_by_text(driver)
            if _clicked:
                time.sleep(0.5)
        seller_ok = is_ozon_seller(seller_name, body_text)
        return CheckResult(
            url=url,
            ok=True,
            has_label=has_label,
            seller_ok=seller_ok,
            seller_name=seller_name,
            label_text=label_text,
            error=None,
        )
    except Exception as e:
        return CheckResult(
            url=url,
            ok=False,
            has_label=False,
            seller_ok=None,
            seller_name=None,
            label_text="",
            error=str(e),
        )
    finally:
        try:
            driver.quit()
        except Exception:
            pass
