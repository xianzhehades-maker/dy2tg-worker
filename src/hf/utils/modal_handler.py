"""通用的登录弹窗检测和关闭模块"""
import time


def detect_and_close_modal(page, verbose=True):
    """检测并自动关闭登录/验证弹窗（增强版）
    
    支持处理：
    - 登录弹窗
    - 验证码弹窗
    - 滑块验证弹窗
    - 二维码登录弹窗
    - 其他类型的模态窗口
    
    Args:
        page: Playwright页面对象
        verbose: 是否打印详细日志
        
    Returns:
        bool: True表示成功处理或未检测到弹窗，False表示处理失败
    """
    try:
        if verbose:
            print("🔍 检查是否存在登录/验证弹窗...")
        
        login_modal_selectors = [
            'div[class*="login-modal"]',
            'div[class*="LoginModal"]',
            'div[class*="login-popup"]',
            'div[class*="LoginPopup"]',
            'div[class*="login-dialog"]',
            'div[class*="LoginDialog"]',
            'div[class*="login-container"]',
            'div[class*="LoginContainer"]',
            'div[data-e2e="login-modal"]',
            'div[data-e2e="login-popup"]',
            'div[data-e2e="login-guide"]',
            'div[aria-label*="登录"]',
            'div[aria-label*="Login"]',
            'div[aria-label*="请登录"]',
            'div[aria-label*="Sign in"]',
            'div[class*="modal"][class*="login"]',
            'div[class*="Modal"][class*="Login"]',
            'div[class*="popup"][class*="login"]',
            'div[class*="Popup"][class*="Login"]',
            'div[class*="verify-modal"]',
            'div[class*="captcha-modal"]',
            'div[class*="verify-popup"]',
            'div[class*="captcha-popup"]',
            'div[data-e2e="verify-modal"]',
            'div[data-e2e="captcha-modal"]',
            'div[class*="slide-verify"]',
            'div[class*="slider-verify"]',
            'div[class*="verify-slider"]',
            'div[class*="qrcode-login"]',
            'div[class*="qr-login"]',
            'div[class*="scan-login"]',
            'div[class*="modal-overlay"]',
            'div[class*="ModalOverlay"]',
            'div[class*="popup-overlay"]',
            'div[class*="PopupOverlay"]',
        ]
        
        close_button_selectors = [
            'button[aria-label*="关闭"]',
            'button[aria-label*="Close"]',
            'button[aria-label*="关闭登录"]',
            'button[aria-label*="Close login"]',
            'button[aria-label*="取消"]',
            'button[aria-label*="Cancel"]',
            'button[class*="close-btn"]',
            'button[class*="closeBtn"]',
            'button[class*="close-button"]',
            'button[class*="closeButton"]',
            'div[class*="close-btn"]',
            'div[class*="closeBtn"]',
            'span[class*="close-btn"]',
            'span[class*="closeBtn"]',
            'svg[class*="close"]',
            'i[class*="close"]',
            '[data-e2e="close-button"]',
            '[data-e2e="close-btn"]',
            '[data-e2e="modal-close"]',
            'button:has-text("关闭")',
            'button:has-text("取消")',
            'button:has-text("跳过")',
            'button:has-text("稍后再说")',
            'button:has-text("暂不登录")',
            'div:has-text("关闭")',
            'span:has-text("关闭")',
            'button:has(svg[class*="close"])',
            'button:has(i[class*="close"])',
            'div:has(svg[class*="close"])',
            'span:has(svg[class*="close"])',
        ]
        
        modal_found = False
        closed_successfully = False
        
        for selector in login_modal_selectors:
            try:
                modal = page.query_selector(selector)
                if modal and modal.is_visible():
                    if verbose:
                        print(f"🎯 检测到弹窗: {selector}")
                    modal_found = True
                    
                    for close_selector in close_button_selectors:
                        try:
                            close_button = modal.query_selector(close_selector)
                            if close_button and close_button.is_visible():
                                if verbose:
                                    print(f"✅ 找到关闭按钮: {close_selector}")
                                close_button.click()
                                page.wait_for_timeout(800)
                                
                                if not modal.is_visible():
                                    if verbose:
                                        print("✅ 登录弹窗已成功关闭（点击关闭按钮）")
                                    closed_successfully = True
                                    return True
                                else:
                                    if verbose:
                                        print("⚠️ 弹窗仍然可见，尝试其他关闭方式")
                                    continue
                        except Exception as e:
                            continue
                    
                    if not closed_successfully and modal.is_visible():
                        if verbose:
                            print("⚠️ 未找到有效的关闭按钮，尝试其他关闭方式")
                        
                        try:
                            page.mouse.click(10, 10)
                            page.wait_for_timeout(500)
                            
                            if not modal.is_visible():
                                if verbose:
                                    print("✅ 通过点击外部区域关闭弹窗成功")
                                closed_successfully = True
                                return True
                        except:
                            pass
                        
                        try:
                            page.keyboard.press('Escape')
                            page.wait_for_timeout(500)
                            
                            if not modal.is_visible():
                                if verbose:
                                    print("✅ 通过按ESC键关闭弹窗成功")
                                closed_successfully = True
                                return True
                        except:
                            pass
                        
                        try:
                            viewport = page.viewport_size
                            if viewport:
                                page.mouse.click(viewport['width'] - 10, viewport['height'] - 10)
                                page.wait_for_timeout(500)
                                
                                if not modal.is_visible():
                                    if verbose:
                                        print("✅ 通过点击右下角区域关闭弹窗成功")
                                    closed_successfully = True
                                    return True
                        except:
                            pass
                    
                    break
            except Exception as e:
                continue
        
        if not modal_found:
            if verbose:
                print("✅ 未检测到登录/验证弹窗")
            return True
        
        if closed_successfully:
            return True
        
        if verbose:
            print("⚠️ 无法关闭登录弹窗，但将继续执行任务")
        return True
        
    except Exception as e:
        if verbose:
            print(f"⚠️ 检测/关闭登录弹窗时出错: {e}")
        return True


def periodic_modal_check(page, interval=3, callback=None):
    """周期性检查并关闭弹窗
    
    Args:
        page: Playwright页面对象
        interval: 检查间隔（秒）
        callback: 每次检查后的回调函数
    """
    while True:
        try:
            detect_and_close_modal(page, verbose=False)
            if callback:
                callback()
            time.sleep(interval)
        except Exception as e:
            print(f"⚠️ 周期性弹窗检查出错: {e}")
            time.sleep(interval)
