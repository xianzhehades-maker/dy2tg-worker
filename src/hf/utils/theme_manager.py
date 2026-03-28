"""主题管理器"""
import tkinter as tk
from tkinter import ttk
import json
import os
from typing import Dict, Callable, List

class ThemeManager:
    """主题管理器 - 管理日间/夜间主题切换"""
    
    THEMES = {
        "light": {
            "name": "日间模式",
            "colors": {
                "bg_primary": "#f0f0f0",
                "bg_secondary": "#ffffff",
                "bg_tertiary": "#e0e0e0",
                "fg_primary": "#333333",
                "fg_secondary": "#666666",
                "accent": "#4a90e2",
                "accent_hover": "#357abd",
                "success": "#28a745",
                "warning": "#ffc107",
                "error": "#dc3545",
                "border": "#cccccc",
                "tree_bg": "#ffffff",
                "tree_fg": "#333333",
                "tree_selected_bg": "#4a90e2",
                "tree_selected_fg": "#ffffff",
                "tree_heading_bg": "#4a90e2",
                "tree_heading_fg": "#ffffff",
                "text_bg": "#ffffff",
                "text_fg": "#333333",
                "text_insert": "#333333",
                "frame_bg": "#f0f0f0",
                "labelframe_bg": "#f0f0f0",
                "notebook_bg": "#e0e0e0",
                "tab_bg": "#e0e0e0",
                "tab_selected_bg": "#4a90e2",
                "tab_fg": "#333333",
                "tab_selected_fg": "#ffffff",
                "button_bg": "#e0e0e0",
                "button_fg": "#333333",
                "scrollbar_bg": "#e0e0e0",
                "scrollbar_trough": "#f0f0f0",
                "statusbar_bg": "#e0e0e0",
                "statusbar_fg": "#333333",
            }
        },
        "dark": {
            "name": "夜间模式",
            "colors": {
                "bg_primary": "#1e1e1e",
                "bg_secondary": "#2d2d2d",
                "bg_tertiary": "#3d3d3d",
                "fg_primary": "#e0e0e0",
                "fg_secondary": "#b0b0b0",
                "accent": "#4a90e2",
                "accent_hover": "#5a9ff2",
                "success": "#3fb950",
                "warning": "#d29922",
                "error": "#f85149",
                "border": "#404040",
                "tree_bg": "#2d2d2d",
                "tree_fg": "#e0e0e0",
                "tree_selected_bg": "#4a90e2",
                "tree_selected_fg": "#ffffff",
                "tree_heading_bg": "#3d3d3d",
                "tree_heading_fg": "#e0e0e0",
                "text_bg": "#2d2d2d",
                "text_fg": "#e0e0e0",
                "text_insert": "#e0e0e0",
                "frame_bg": "#1e1e1e",
                "labelframe_bg": "#1e1e1e",
                "notebook_bg": "#2d2d2d",
                "tab_bg": "#2d2d2d",
                "tab_selected_bg": "#4a90e2",
                "tab_fg": "#b0b0b0",
                "tab_selected_fg": "#ffffff",
                "button_bg": "#3d3d3d",
                "button_fg": "#e0e0e0",
                "scrollbar_bg": "#3d3d3d",
                "scrollbar_trough": "#2d2d2d",
                "statusbar_bg": "#2d2d2d",
                "statusbar_fg": "#e0e0e0",
            }
        }
    }
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_file: str = "config/theme.json"):
        if ThemeManager._initialized:
            return
        
        ThemeManager._initialized = True
        self.config_file = config_file
        self.current_theme = "light"
        self._callbacks: List[Callable] = []
        self._root = None
        self._style = None
        
        self._load_theme()
    
    def _load_theme(self):
        """从配置文件加载主题"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.current_theme = data.get("theme", "light")
            except Exception:
                self.current_theme = "light"
    
    def _save_theme(self):
        """保存主题到配置文件"""
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump({"theme": self.current_theme}, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    
    def initialize(self, root: tk.Tk, style: ttk.Style):
        """初始化主题管理器"""
        self._root = root
        self._style = style
        self._apply_theme()
    
    def get_current_theme(self) -> str:
        """获取当前主题名称"""
        return self.current_theme
    
    def get_theme_name(self) -> str:
        """获取当前主题显示名称"""
        return self.THEMES[self.current_theme]["name"]
    
    def get_colors(self) -> Dict[str, str]:
        """获取当前主题的颜色配置"""
        return self.THEMES[self.current_theme]["colors"]
    
    def get_color(self, key: str) -> str:
        """获取指定颜色"""
        return self.THEMES[self.current_theme]["colors"].get(key, "#000000")
    
    def toggle_theme(self):
        """切换主题"""
        self.current_theme = "dark" if self.current_theme == "light" else "light"
        self._apply_theme()
        self._save_theme()
        self._notify_callbacks()
    
    def set_theme(self, theme_name: str):
        """设置指定主题"""
        if theme_name in self.THEMES:
            self.current_theme = theme_name
            self._apply_theme()
            self._save_theme()
            self._notify_callbacks()
    
    def _apply_theme(self):
        """应用当前主题"""
        if not self._root or not self._style:
            return
        
        colors = self.get_colors()
        
        self._root.configure(bg=colors["bg_primary"])
        
        self._style.theme_use('clam')
        
        self._style.configure('TFrame', background=colors["frame_bg"])
        self._style.configure('TLabel', background=colors["frame_bg"], foreground=colors["fg_primary"], font=('Microsoft YaHei UI', 10))
        self._style.configure('TButton', font=('Microsoft YaHei UI', 10), padding=5, background=colors["button_bg"], foreground=colors["button_fg"])
        self._style.map('TButton', background=[('active', colors["accent_hover"]), ('pressed', colors["accent"])])
        
        self._style.configure('TNotebook', background=colors["notebook_bg"], tabposition='n')
        self._style.configure('TNotebook.Tab', padding=[15, 5], font=('Microsoft YaHei UI', 11, 'bold'), background=colors["tab_bg"], foreground=colors["tab_fg"])
        self._style.map('TNotebook.Tab', 
            background=[('selected', colors["tab_selected_bg"]), ('active', colors["accent_hover"])],
            foreground=[('selected', colors["tab_selected_fg"]), ('active', colors["tab_selected_fg"])]
        )
        
        self._style.configure('Treeview', 
            font=('Microsoft YaHei UI', 9), 
            rowheight=25, 
            background=colors["tree_bg"], 
            foreground=colors["tree_fg"],
            fieldbackground=colors["tree_bg"]
        )
        self._style.configure('Treeview.Heading', 
            font=('Microsoft YaHei UI', 10, 'bold'), 
            background=colors["tree_heading_bg"], 
            foreground=colors["tree_heading_fg"]
        )
        self._style.map('Treeview', 
            background=[('selected', colors["tree_selected_bg"])], 
            foreground=[('selected', colors["tree_selected_fg"])]
        )
        
        self._style.configure('TLabelframe', background=colors["labelframe_bg"])
        self._style.configure('TLabelframe.Label', background=colors["labelframe_bg"], foreground=colors["fg_primary"])
        
        self._style.configure('TEntry', fieldbackground=colors["text_bg"], foreground=colors["text_fg"])
        self._style.configure('TCombobox', fieldbackground=colors["text_bg"], foreground=colors["text_fg"])
        self._style.configure('TSpinbox', fieldbackground=colors["text_bg"], foreground=colors["text_fg"])
        
        self._style.configure('TCheckbutton', background=colors["frame_bg"], foreground=colors["fg_primary"])
        self._style.configure('TRadiobutton', background=colors["frame_bg"], foreground=colors["fg_primary"])
        
        self._style.configure('TScrollbar', background=colors["scrollbar_bg"], troughcolor=colors["scrollbar_trough"])
        
        self._style.configure('Horizontal.TProgressbar', background=colors["accent"], troughcolor=colors["bg_tertiary"])
        
        self._style.configure('Status.TLabel', background=colors["statusbar_bg"], foreground=colors["statusbar_fg"])
        
        self._apply_to_children(self._root, colors)
    
    def _apply_to_children(self, widget, colors: Dict[str, str]):
        """递归应用主题到所有子组件"""
        widget_type = widget.winfo_class()
        
        if widget_type == 'Text':
            try:
                widget.configure(
                    bg=colors["text_bg"],
                    fg=colors["text_fg"],
                    insertbackground=colors["text_insert"],
                    selectbackground=colors["accent"],
                    selectforeground=colors["tree_selected_fg"]
                )
            except tk.TclError:
                pass
        
        elif widget_type == 'Menu':
            try:
                widget.configure(
                    bg=colors["bg_secondary"],
                    fg=colors["fg_primary"],
                    activebackground=colors["accent"],
                    activeforeground=colors["tree_selected_fg"]
                )
            except tk.TclError:
                pass
        
        elif widget_type == 'Canvas':
            try:
                widget.configure(
                    bg=colors["bg_primary"]
                )
            except tk.TclError:
                pass
        
        elif widget_type == 'Listbox':
            try:
                widget.configure(
                    bg=colors["tree_bg"],
                    fg=colors["tree_fg"],
                    selectbackground=colors["tree_selected_bg"],
                    selectforeground=colors["tree_selected_fg"]
                )
            except tk.TclError:
                pass
        
        for child in widget.winfo_children():
            self._apply_to_children(child, colors)
    
    def register_callback(self, callback: Callable):
        """注册主题变更回调"""
        if callback not in self._callbacks:
            self._callbacks.append(callback)
    
    def unregister_callback(self, callback: Callable):
        """取消注册主题变更回调"""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
    
    def _notify_callbacks(self):
        """通知所有回调"""
        for callback in self._callbacks:
            try:
                callback(self.current_theme)
            except Exception:
                pass
    
    def refresh_theme(self):
        """刷新主题（重新应用当前主题）"""
        self._apply_theme()
        self._notify_callbacks()
    
    def apply_to_dialog(self, dialog: tk.Toplevel):
        """应用主题到对话框"""
        colors = self.get_colors()
        dialog.configure(bg=colors["bg_primary"])
        self._apply_to_children(dialog, colors)


_theme_manager = None

def get_theme_manager() -> ThemeManager:
    """获取主题管理器单例"""
    global _theme_manager
    if _theme_manager is None:
        _theme_manager = ThemeManager()
    return _theme_manager
