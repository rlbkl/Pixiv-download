import os
import time
import random
import requests
import threading
import traceback
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, SessionNotCreatedException
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置
TARGET_AUTHOR_ID = "ID"  # 替换为目标 Pixiv ID
DOWNLOAD_BASE_DIR = "pixiv_downloads"  # 下载基础目录
COOKIES_FILE = "cookie.json"  # Cookies 文件
MAX_THREADS = 5  # 最大下载线程数
MAX_BROWSER_THREADS = 2  # 最大浏览器线程数
REQUEST_DELAY = 3  # 请求间隔（秒）
POSTS_PER_PAGE = 18  # 每页最大帖子数
USER_AGENT = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 EdgA/130.0.0.0"
HEADLESS = ""  # --headless 是否显示浏览器页面
MAX_RETRIES = 3  # 最大重试次数
RETRY_INTERVAL = 1.5  # 重试间隔（秒）
RETRY_TIMEOUT = 4.5  # 重试总时长（秒）

# 全局变量
post_image_counts = {}
post_image_counts_lock = threading.Lock()
global_failed_images = []
global_failed_posts = []
global_failed_posts_lock = threading.Lock()
skipped_posts = []  # 记录跳过的帖子

# 初始化主浏览器
options = webdriver.ChromeOptions()
options.add_argument(f"user-agent={USER_AGENT}")
if HEADLESS:
    options.add_argument(HEADLESS)
options.add_argument("--disable-gpu")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--no-sandbox")
driver = webdriver.Chrome(options=options)

# 创建基础下载目录
if not os.path.exists(DOWNLOAD_BASE_DIR):
    os.makedirs(DOWNLOAD_BASE_DIR)

# 清理文件名
def sanitize_filename(filename):
    # Windows 非法字符
    invalid_chars = r'[<>:"/\\|?*]'
    # 替换非法字符为 _
    clean_filename = re.sub(invalid_chars, '_', filename)
    # 移除控制字符
    clean_filename = re.sub(r'[\x00-\x1F\x7F]', '', clean_filename)
    # 移除首尾空格和点
    clean_filename = clean_filename.strip().strip('.')
    # 若为空，使用默认名
    if not clean_filename:
        clean_filename = "Untitled"
    return clean_filename

# 加载 cookies
def load_cookies(driver):
    if not os.path.exists(COOKIES_FILE):
        raise FileNotFoundError(f"{COOKIES_FILE} 不存在，请使用 Cookie-Editor 插件获取 Pixiv 的 cookies，导出为 Header String 格式并保存到 {COOKIES_FILE} 文件！")

    print("正在加载 Cookies...")
    with open(COOKIES_FILE, "r", encoding="utf-8") as f:
        cookie_str = f.read().strip()
    if not cookie_str:
        raise ValueError(f"{COOKIES_FILE} 文件为空，请使用 Cookie-Editor 插件获取 Pixiv 的 cookies，导出为 Header String 格式并保存到 {COOKIES_FILE} 文件！")

    driver.get("https://www.pixiv.net")
    cookies = []
    for cookie_pair in cookie_str.split(";"):
        if "=" in cookie_pair:
            name, value = cookie_pair.split("=", 1)
            cookies.append({"name": name.strip(), "value": value.strip(), "domain": ".pixiv.net"})

    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
        except Exception as e:
            print(f"添加 cookie {cookie['name']} 失败: {e}")

    driver.refresh()
    time.sleep(2)
    if "投稿作品" in driver.page_source:
        print("Cookies 加载成功，已登录！")
        return True
    else:
        raise ValueError(f"{COOKIES_FILE} 加载失败，请检查 cookies 是否有效！请使用 Cookie-Editor 插件获取 Pixiv 的 cookies，导出为 Header String 格式并保存到 {COOKIES_FILE} 文件！")

# 获取作者信息
def get_author_info():
    driver.get(f"https://www.pixiv.net/users/{TARGET_AUTHOR_ID}/artworks")
    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        author_name_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1.f-title-xs"))
        )
        author_name = author_name_element.text.strip()
        download_dir = os.path.join(DOWNLOAD_BASE_DIR, sanitize_filename(f"{author_name}_{TARGET_AUTHOR_ID}"))
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)
        return download_dir
    except TimeoutException:
        print("无法获取作者信息，请检查网络或作者 ID！")
        driver.quit()
        exit(1)

# 下载单张图片（单次尝试）
def download_image(url, filepath):
    headers = {"User-Agent": USER_AGENT}
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=10)
        if response.status_code == 200:
            with open(filepath, "wb") as f:
                total_size = 0
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
            # 验证文件完整性
            if total_size > 0:
                print(f"已下载图片: {filepath} ({total_size} bytes)")
                return True
            else:
                print(f"下载失败: {url} (文件为空)")
                return False
        else:
            print(f"下载失败: {url} (状态码: {response.status_code})")
            return False
    except requests.RequestException as e:
        print(f"下载出错: {url} ({e})")
        return False
    except OSError as e:
        print(f"保存文件 {filepath} 时出错: {e}")
        return False

# 尝试下载单张图片（包括重试）
def download_with_retries(post_id, idx, jpg_url, png_url, filepath_jpg, filepath_png):
    for attempt in range(1, MAX_RETRIES + 1):
        # 先尝试 JPG
        if download_image(jpg_url, filepath_jpg):
            return True
        time.sleep(RETRY_INTERVAL)  # 等待 1.5 秒
        print(f"下载失败: {jpg_url}，尝试 {attempt}/{MAX_RETRIES}")

        # 再尝试 PNG
        if download_image(png_url, filepath_png):
            return True
        time.sleep(RETRY_INTERVAL)  # 等待 1.5 秒
        print(f"下载失败: {png_url}，尝试 {attempt}/{MAX_RETRIES}")

    return False

# 初始化浏览器
def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={USER_AGENT}")
    if HEADLESS:
        options.add_argument(HEADLESS)
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    try:
        driver = webdriver.Chrome(options=options)
        try:
            load_cookies(driver)
            return driver
        except Exception as e:
            driver.quit()
            return None
    except (WebDriverException, SessionNotCreatedException) as e:
        print(f"初始化浏览器失败: {e}")
        return None

# 收集帖子数据
def collect_post_data(download_dir):
    post_data = []
    page = 1
    total_skipped = 0

    while True:
        driver.get(f"https://www.pixiv.net/users/{TARGET_AUTHOR_ID}/artworks?p={page}")
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.list-item.column-2"))
            )
            post_elements = driver.find_elements(By.CSS_SELECTOR, "div.list-item.column-2")
            current_page_posts = []
            page_skipped = 0

            for element in post_elements:
                try:
                    # 提取帖子 ID
                    works_item = element.find_element(By.CSS_SELECTOR, "div.works-item-illust")
                    post_id = works_item.get_attribute("data-tx")

                    # 提取标题
                    thumb_img = element.find_element(By.CSS_SELECTOR, "img.thumb")
                    alt_text = thumb_img.get_attribute("alt")
                    # 格式：#标签 标题 - 作者
                    title_match = re.match(r"#.*?\s+(.*?)\s*-\s*.*", alt_text)
                    title = title_match.group(1) if title_match else f"Untitled_{post_id}"
                    original_title = title
                    title = sanitize_filename(title)
                    if original_title != title:
                        print(f"清理文件名: {original_title} -> {title}")

                    # 提取图片数量
                    try:
                        page_count_span = element.find_element(By.CSS_SELECTOR, "span[data-v-5e6e10b7]")
                        image_count = int(page_count_span.text.strip())
                    except NoSuchElementException:
                        image_count = 1

                    # 提取日期路径
                    src = thumb_img.get_attribute("src")
                    date_path_match = re.search(r"/(\d{4}/\d{2}/\d{2}/\d{2}/\d{2}/\d{2}/\d+_p0)", src)
                    date_path = date_path_match.group(1) if date_path_match else None
                    if not date_path:
                        print(f"帖子 {post_id} 无法提取日期路径（可能是 GIF 或无效格式），跳过")
                        skipped_posts.append(post_id)
                        page_skipped += 1
                        continue

                    # 生成图片 URL
                    image_urls = []
                    for i in range(image_count):
                        # 从 _p0 开始
                        suffix = f"_p{i}"
                        png_url = f"https://i.pixiv.cat/img-original/img/{date_path.replace('_p0', suffix)}.png"
                        jpg_url = f"https://i.pixiv.cat/img-original/img/{date_path.replace('_p0', suffix)}.jpg"
                        filename = f"{title}_{post_id}" if i == 0 else f"{title}_{post_id}_p{i}"
                        filepath_png = os.path.join(download_dir, f"{filename}.png")
                        filepath_jpg = os.path.join(download_dir, f"{filename}.jpg")

                        if os.path.exists(filepath_png) or os.path.exists(filepath_jpg):
                            print(f"图片已存在，跳过: {filename}")
                            continue

                        image_urls.append((post_id, i, png_url, jpg_url, filepath_png, filepath_jpg))

                    with post_image_counts_lock:
                        post_image_counts[post_id] = image_count

                    current_page_posts.append({
                        "post_id": post_id,
                        "title": title,
                        "image_count": image_count,
                        "date_path": date_path,
                        "image_urls": image_urls
                    })

                except Exception as e:
                    print(f"处理帖子元素 {post_id} 时出错: {e}")
                    skipped_posts.append(post_id)
                    page_skipped += 1
                    continue

            post_data.extend(current_page_posts)
            total_skipped += page_skipped
            print(f"第 {page} 页发现 {len(post_elements)} 个帖子，有效帖子 {len(current_page_posts)} 个，跳过帖子 {page_skipped} 个")

            if len(post_elements) >= POSTS_PER_PAGE:
                page += 1
            else:
                break

        except TimeoutException:
            print(f"无法加载第 {page} 页的帖子列表，请检查网络或页面结构！")
            break

        time.sleep(REQUEST_DELAY)

    print(f"\n最终收集到 {len(post_data)} 个有效帖子，跳过 {total_skipped} 个帖子")
    if skipped_posts:
        print(f"跳过的帖子 ID: {skipped_posts}")
    return post_data

# 处理一组帖子数据
def process_post_chunk(post_chunk, download_dir):
    image_urls = []
    for post in post_chunk:
        try:
            image_urls.extend(post["image_urls"])
        except Exception as e:
            print(f"处理帖子 {post['post_id']} 时出错: {e}")
            with global_failed_posts_lock:
                global_failed_posts.append(post['post_id'])
    return image_urls

# 下载所有图片
def download_all_images(post_data, download_dir):
    global global_failed_images, global_failed_posts

    print("\n多线程收集所有帖子图片 URL...")
    all_image_urls = []
    chunk_size = max(1, len(post_data) // MAX_BROWSER_THREADS)
    post_chunks = [post_data[i:i + chunk_size] for i in range(0, len(post_data), chunk_size)]

    with ThreadPoolExecutor(max_workers=MAX_BROWSER_THREADS) as executor:
        futures = [executor.submit(process_post_chunk, chunk, download_dir) for chunk in post_chunks]
        for future in as_completed(futures):
            try:
                image_urls = future.result()
                all_image_urls.extend(image_urls)
            except Exception as e:
                print(f"线程处理出错: {e}")
                traceback.print_exc()

    # 重试失败的帖子
    while global_failed_posts:
        print(f"\n有 {len(global_failed_posts)} 个帖子处理失败：{global_failed_posts}")
        user_input = input("是否尝试重新处理这些帖子？(y/n): ").strip().lower()
        if user_input != 'y':
            print("用户选择跳过失败的帖子")
            break

        failed_posts = global_failed_posts.copy()
        global_failed_posts = []
        failed_post_data = [p for p in post_data if p["post_id"] in failed_posts]
        chunk_size = max(1, len(failed_post_data) // MAX_BROWSER_THREADS)
        post_chunks = [failed_post_data[i:i + chunk_size] for i in range(0, len(failed_post_data), chunk_size)]

        with ThreadPoolExecutor(max_workers=MAX_BROWSER_THREADS) as executor:
            futures = [executor.submit(process_post_chunk, chunk, download_dir) for chunk in post_chunks]
            for future in as_completed(futures):
                try:
                    image_urls = future.result()
                    all_image_urls.extend(image_urls)
                except Exception as e:
                    print(f"线程处理出错: {e}")
                    traceback.print_exc()

    print(f"\n共收集到 {len(all_image_urls)} 张图片，准备分批下载...")
    downloaded_count = 0

    # 分批下载，每批最多 MAX_THREADS 张图片
    for batch_idx, start in enumerate(range(0, len(all_image_urls), MAX_THREADS)):
        batch = all_image_urls[start:start + MAX_THREADS]
        print(f"\n处理下载批次 {batch_idx + 1}（{len(batch)} 张图片）...")

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_image = {}
            for post_id, idx, png_url, jpg_url, filepath_png, filepath_jpg in batch:
                # 提交单张图片的下载任务（包括重试）
                future = executor.submit(
                    download_with_retries,
                    post_id, idx, jpg_url, png_url, filepath_jpg, filepath_png
                )
                future_to_image[future] = (post_id, idx, filepath_png.split('.png')[0])

            # 等待当前批次所有任务完成
            for future in as_completed(future_to_image):
                post_id, idx, filepath_base = future_to_image[future]
                try:
                    if future.result():
                        downloaded_count += 1
                    else:
                        global_failed_images.append((post_id, idx, filepath_base))
                except Exception as e:
                    print(f"下载 {filepath_base} 时出错: {e}")
                    global_failed_images.append((post_id, idx, filepath_base))

        print(f"批次 {batch_idx + 1} 下载完成！")

    # 重试失败的图片
    while global_failed_images:
        print(f"\n有 {len(global_failed_images)} 张图片下载失败：{[f'{post_id}_p{idx}' for post_id, idx, _ in global_failed_images]}")
        user_input = input("是否尝试重新下载这些图片？(y/n): ").strip().lower()
        if user_input != 'y':
            print("用户选择跳过失败的图片")
            break

        remaining_failed = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_image = {}
            for post_id, idx, filepath_base in global_failed_images:
                for post in post_data:
                    if post["post_id"] == post_id:
                        for img_url in post["image_urls"]:
                            if img_url[0] == post_id and img_url[1] == idx:
                                png_url, jpg_url = img_url[2], img_url[3]
                                filepath_png = img_url[4]
                                filepath_jpg = img_url[5]
                                future = executor.submit(
                                    download_with_retries,
                                    post_id, idx, jpg_url, png_url, filepath_jpg, filepath_png
                                )
                                future_to_image[future] = (post_id, idx, filepath_base)
                                break

            for future in as_completed(future_to_image):
                post_id, idx, filepath_base = future_to_image[future]
                try:
                    if future.result():
                        downloaded_count += 1
                    else:
                        remaining_failed.append((post_id, idx, filepath_base))
                except Exception as e:
                    print(f"下载 {filepath_base} 时出错: {e}")
                    remaining_failed.append((post_id, idx, filepath_base))

        global_failed_images = remaining_failed

    print("\n最终帖子及其图片数量：")
    total_posts = len(post_data)  # 使用 post_data 长度
    total_images = sum([count for count in post_image_counts.values() if isinstance(count, int)])
    for post in post_data:
        count = post_image_counts.get(post["post_id"], "未知")
        print(f"帖子 ID: {post['post_id']}, 图片数量: {count}")
    print(f"\n统计：共 {total_posts} 个帖子，{total_images} 张图片")

    print(f"下载完成！共需下载 {len(all_image_urls)} 张图片，实际下载 {downloaded_count} 张图片")

# 主函数
def main():
    try:
        load_cookies(driver)
    except (FileNotFoundError, ValueError) as e:
        print(f"错误: {e}")
        driver.quit()
        exit(1)

    download_dir = get_author_info()
    post_data = collect_post_data(download_dir)
    print(f"收集到 {len(post_data)} 个有效帖子")

    if not post_data:
        print("没有收集到任何有效帖子，程序退出！")
        driver.quit()
        return

    download_all_images(post_data, download_dir)
    driver.quit()

if __name__ == "__main__":
    main()