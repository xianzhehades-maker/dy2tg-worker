"""Hugging Face Dataset 持久化存储模块"""
import os
import sys
import json
import shutil
import sqlite3
import time
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Any
from pathlib import Path

try:
    from datasets import load_dataset, Dataset, DatasetDict
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False
    print("⚠️ datasets 库未安装，请运行: pip install datasets")

from hf.utils.logger import get_logger

logger = get_logger("hf_persistence")

class HuggingFacePersistence:
    """Hugging Face Dataset 持久化管理器"""
    
    _lock_file = None
    _lock_held = False
    
    def __init__(
        self,
        repo_id: str,
        hf_token: str,
        db_path: str = None,
        data_dir: str = None
    ):
        """
        初始化持久化管理器
        
        Args:
            repo_id: Hugging Face Dataset 仓库 ID (格式: 用户名/数据集名)
            hf_token: Hugging Face 访问令牌
            db_path: SQLite 数据库文件路径
            data_dir: 数据目录路径
        """
        self.repo_id = repo_id
        self.hf_token = hf_token
        self.db_path = db_path or self._get_default_db_path()
        self.data_dir = data_dir or self._get_default_data_dir()
        self._lock_path = self.db_path + ".sync_lock"
        
        if not HF_AVAILABLE:
            raise ImportError("datasets 库未安装")
        
        logger.info(f"初始化 Hugging Face 持久化: {repo_id}")
    
    def _get_default_db_path(self) -> str:
        """获取默认数据库路径"""
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(parent_dir, "data", "tasks.db")
    
    def _get_default_data_dir(self) -> str:
        """获取默认数据目录"""
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(parent_dir, "data")
    
    def _load_sqlite_to_dataframes(self) -> Dict[str, pd.DataFrame]:
        """
        将 SQLite 数据库加载为 DataFrame 字典

        Returns:
            表名 -> DataFrame 的字典
        """
        if not os.path.exists(self.db_path):
            logger.warning(f"数据库文件不存在: {self.db_path}")
            return {}

        conn = sqlite3.connect(self.db_path)

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            dataframes = {}
            for table in tables:
                if table.startswith("sqlite_"):
                    logger.info(f"跳过 SQLite 内部表: {table}")
                    continue

                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)

                logger.info(f"[HF_PERSISTENCE] 加载表 {table}: {len(df)} 条记录")
                logger.info(f"[HF_PERSISTENCE] 表 {table} 的列: {list(df.columns)}")

                if 'id' in df.columns:
                    logger.info(f"[HF_PERSISTENCE] 表 {table} 的 id 列类型(读取后): {df['id'].dtype}")
                    logger.info(f"[HF_PERSISTENCE] 表 {table} 的 id 列前5个值(读取后): {df['id'].head().tolist()}")

                    if df['id'].isna().any():
                        logger.warning(f"[HF_PERSISTENCE] 表 {table} 的 id 列存在 NaN，共 {df['id'].isna().sum()} 个")
                        df = self._fix_dataframe_ids(df, table)
                        logger.info(f"[HF_PERSISTENCE] 表 {table} 的 id 列修复后前5个值: {df['id'].head().tolist()}")

                dataframes[table] = df

            return dataframes
        finally:
            conn.close()

    def _fix_dataframe_ids(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """修复 DataFrame 中的 id 列，处理 NaN 值 - 直接重新生成连续 id"""
        df = df.copy()

        if 'id' not in df.columns:
            return df

        logger.warning(f"[HF_PERSISTENCE] 正在修复表 {table_name} 的 id 列...")

        if df['id'].dtype == 'float64' or df['id'].dtype == 'float32' or df['id'].dtype == 'object':
            nan_count = df['id'].isna().sum()
            non_nan_count = df['id'].notna().sum()
            logger.warning(f"[HF_PERSISTENCE] id列类型: {df['id'].dtype}, NaN数量: {nan_count}, 非NaN数量: {non_nan_count}")

            if non_nan_count > 0:
                try:
                    max_id = int(float(df['id'].max()))
                    logger.warning(f"[HF_PERSISTENCE] 当前最大id: {max_id}")
                except:
                    max_id = 0
                    logger.warning(f"[HF_PERSISTENCE] 无法获取最大id，设为0")
            else:
                max_id = 0
                logger.warning(f"[HF_PERSISTENCE] 所有id都是NaN，将重新生成")

            id_counter = max_id + 1

            nan_mask = df['id'].isna()
            nan_indices = df[nan_mask].index.tolist()
            logger.warning(f"[HF_PERSISTENCE] 需要修复的 NaN id 行索引: {nan_indices[:10]}...")

            for idx in nan_indices:
                df.at[idx, 'id'] = id_counter
                id_counter += 1

            valid_mask = df['id'].notna()
            for idx in df[valid_mask].index:
                try:
                    val = int(float(df.at[idx, 'id']))
                    df.at[idx, 'id'] = val
                except:
                    df.at[idx, 'id'] = id_counter
                    id_counter += 1

            logger.warning(f"[HF_PERSISTENCE] 修复后 id 列前10个值: {df['id'].head(10).tolist()}")

        dtype_str = str(df['id'].dtype)
        if dtype_str == 'Int64' or dtype_str == 'int64':
            df['id'] = df['id'].astype('int64')
        else:
            df['id'] = df['id'].astype('int64')
        return df
    
    def _save_dataframes_to_sqlite(self, dataframes: Dict[str, pd.DataFrame]):
        """
        将 DataFrame 字典保存回 SQLite 数据库（安全合并模式，绝不删除表）

        Args:
            dataframes: 表名 -> DataFrame 的字典
        """
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        conn = sqlite3.connect(self.db_path)

        try:
            for table_name, df in dataframes.items():
                if table_name.startswith("sqlite_"):
                    logger.info(f"跳过 SQLite 内部表: {table_name}")
                    continue

                logger.info(f"[HF_PERSISTENCE] 准备保存表 {table_name}: {len(df)} 条记录")

                df_to_save = df

                if 'id' in df.columns:
                    dtype_str = str(df['id'].dtype)
                    if df['id'].isna().any() or dtype_str in ['float64', 'float32', 'object', 'Int64']:
                        logger.warning(f"[HF_PERSISTENCE] 表 {table_name} 需要修复 id 列")
                        df_to_save = self._fix_dataframe_ids(df.copy(), table_name)

                if 'id' not in df_to_save.columns or df_to_save['id'].isna().all():
                    logger.error(f"[HF_PERSISTENCE] 严重错误：表 {table_name} 的 id 列全部为 NaN/None，拒绝保存！")
                    logger.error(f"[HF_PERSISTENCE] 这会覆盖本地有效数据！跳过此表。")
                    continue

                # 检查表是否存在
                cursor = conn.cursor()
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                table_exists = cursor.fetchone() is not None

                if not table_exists:
                    logger.info(f"[HF_PERSISTENCE] 表 {table_name} 不存在，直接创建")
                    df_to_save.to_sql(table_name, conn, index=False)
                    logger.info(f"[HF_PERSISTENCE] 表 {table_name} 创建成功: {len(df_to_save)} 条记录")
                    continue

                # 表存在，进行安全合并
                logger.info(f"[HF_PERSISTENCE] 表 {table_name} 已存在，进行安全合并")

                # 1. 获取本地现有数据的 ID
                existing_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                existing_ids = set(existing_df['id'].dropna().tolist()) if 'id' in existing_df.columns else set()
                logger.info(f"[HF_PERSISTENCE] 表 {table_name} 本地有 {len(existing_df)} 条记录")

                # 2. 分离新增数据和更新数据
                if 'id' in df_to_save.columns:
                    df_new = df_to_save[~df_to_save['id'].isin(existing_ids)]
                    df_update = df_to_save[df_to_save['id'].isin(existing_ids)]
                else:
                    df_new = df_to_save
                    df_update = pd.DataFrame()

                logger.info(f"[HF_PERSISTENCE] 表 {table_name}: 新增 {len(df_new)} 条，待更新 {len(df_update)} 条")

                # 3. 插入新数据
                if not df_new.empty:
                    df_new.to_sql(table_name, conn, index=False, if_exists='append')
                    logger.info(f"[HF_PERSISTENCE] 表 {table_name}: 已插入 {len(df_new)} 条新记录")

                # 4. 更新现有数据（逐行更新，安全）
                if not df_update.empty and 'id' in df_update.columns:
                    update_count = 0
                    for _, row in df_update.iterrows():
                        row_id = row['id']
                        if pd.isna(row_id):
                            continue

                        # 构建 UPDATE 语句
                        set_clause = []
                        values = []
                        for col in df_update.columns:
                            if col == 'id':
                                continue
                            val = row[col]
                            if pd.isna(val):
                                set_clause.append(f'"{col}" = NULL')
                            else:
                                set_clause.append(f'"{col}" = ?')
                                values.append(val)

                        if set_clause:
                            values.append(row_id)
                            sql = f'UPDATE "{table_name}" SET {", ".join(set_clause)} WHERE id = ?'
                            try:
                                cursor.execute(sql, values)
                                update_count += 1
                            except Exception as e:
                                logger.warning(f"[HF_PERSISTENCE] 更新 id={row_id} 失败: {e}")

                    conn.commit()
                    logger.info(f"[HF_PERSISTENCE] 表 {table_name}: 已更新 {update_count} 条记录")

                # 5. 验证结果
                verify_df = pd.read_sql_query(f"SELECT COUNT(*) as cnt FROM {table_name}", conn)
                logger.info(f"[HF_PERSISTENCE] 表 {table_name} 当前总记录数: {verify_df['cnt'].iloc[0]}")

            conn.commit()
            logger.info(f"[HF_PERSISTENCE] 数据库已安全保存: {self.db_path}")
        except Exception as e:
            logger.error(f"[HF_PERSISTENCE] 保存数据库失败: {e}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            conn.close()
    
    def _acquire_lock(self, timeout=60):
        """获取同步锁，防止并发 push/pull"""
        start_time = time.time()
        while True:
            try:
                self._lock_file = open(self._lock_path, 'x')
                self._lock_file.write(f"{os.getpid()}|{datetime.now().isoformat()}")
                self._lock_file.flush()
                self._lock_held = True
                logger.info(f"[HF_PERSISTENCE] 获取同步锁成功: {os.getpid()}")
                return True
            except FileExistsError:
                if time.time() - start_time > timeout:
                    logger.warning(f"[HF_PERSISTENCE] 获取同步锁超时 ({timeout}s)，另一个进程可能正在进行同步")
                    return False
                logger.info("[HF_PERSISTENCE] 同步锁被占用，等待中...")
                time.sleep(2)
    
    def _release_lock(self):
        """释放同步锁"""
        if self._lock_held and self._lock_file:
            try:
                self._lock_file.close()
            except:
                pass
            try:
                os.remove(self._lock_path)
                logger.info("[HF_PERSISTENCE] 释放同步锁")
            except:
                pass
            self._lock_held = False
            self._lock_file = None
    
    def push_to_hub(self, message: str = "Update data") -> bool:
        """
        将本地数据推送到 Hugging Face Dataset
        
        Args:
            message: 提交信息
            
        Returns:
            是否成功
        """
        if not self._acquire_lock():
            logger.warning("[HF_PERSISTENCE] 无法获取同步锁，跳过本次推送")
            return False
        
        try:
            print(f"\n{'='*60}", flush=True)
            print(f"🚀 开始推送到 Hugging Face Dataset...", flush=True)
            print(f"{'='*60}", flush=True)
            logger.info("开始推送到 Hugging Face Dataset...")
            
            # 1. 加载本地数据库
            print("📖 加载本地数据库...", flush=True)
            dataframes = self._load_sqlite_to_dataframes()
            
            if not dataframes:
                print("⚠️ 没有数据可推送", flush=True)
                logger.warning("没有数据可推送")
                return False
            
            print(f"✅ 成功加载 {len(dataframes)} 个表", flush=True)
            
            if not dataframes:
                print("⚠️ 没有数据可推送", flush=True)
                logger.warning("没有数据可推送")
                self._release_lock()
                return False
            
            # 2. 创建临时目录保存 CSV 文件
            import tempfile
            import shutil
            from huggingface_hub import HfApi, upload_folder

            temp_dir = tempfile.mkdtemp()
            try:
                print("💾 保存表为 CSV 文件...", flush=True)
                for table_name, df in dataframes.items():
                    if 'id' in df.columns:
                        if df['id'].isna().any():
                            logger.error(f"[HF_PERSISTENCE] 严重错误：表 {table_name} 的 id 列在上传前仍有 NaN！")
                            logger.error(f"[HF_PERSISTENCE] id列前10个值: {df['id'].head(10).tolist()}")
                            raise ValueError(f"表 {table_name} 的 id 列包含 NaN，无法上传")

                    csv_path = os.path.join(temp_dir, f"{table_name}.csv")
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    print(f"   - {table_name}.csv: {len(df)} 条记录 (id列验证通过)", flush=True)
                
                # 3. 上传到 Hub
                print(f"📤 上传到 Hub: {self.repo_id}...", flush=True)
                
                api = HfApi(token=self.hf_token)
                
                # 确保仓库存在
                try:
                    api.repo_info(self.repo_id, repo_type="dataset")
                except Exception:
                    print(f"   📦 创建数据集仓库...", flush=True)
                    api.create_repo(
                        self.repo_id,
                        repo_type="dataset",
                        private=False,
                        token=self.hf_token,
                        exist_ok=True
                    )
                
                # 上传文件夹
                upload_folder(
                    folder_path=temp_dir,
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    commit_message=message,
                    token=self.hf_token
                )
                
                print(f"\n✅ 成功推送到 Hugging Face Dataset: {self.repo_id}", flush=True)
                print(f"{'='*60}\n", flush=True)
                logger.info(f"✅ 成功推送到 Hugging Face Dataset: {self.repo_id}")
                return True
                
            finally:
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                finally:
                    self._release_lock()
            
        except Exception as e:
            print(f"\n❌ 推送到 Hugging Face 失败: {e}", flush=True)
            print(f"{'='*60}\n", flush=True)
            logger.error(f"❌ 推送到 Hugging Face 失败: {e}")
            import traceback
            traceback.print_exc()
            self._release_lock()
            return False
    
    def pull_from_hub(self) -> bool:
        """
        从 Hugging Face Dataset 拉取数据到本地
        
        Returns:
            是否成功
        """
        if not self._acquire_lock():
            logger.warning("[HF_PERSISTENCE] 无法获取同步锁，跳过本次拉取")
            return False
        
        try:
            print(f"\n{'='*60}", flush=True)
            print(f"📥 从 Hugging Face Dataset 拉取数据...", flush=True)
            print(f"{'='*60}", flush=True)
            logger.info("从 Hugging Face Dataset 拉取数据...")

            import requests
            import tempfile

            headers = {"Authorization": f"Bearer {self.hf_token}"}
            base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main"

            print(f"📥 从 {self.repo_id} 下载数据集...", flush=True)

            list_url = f"https://huggingface.co/api/datasets/{self.repo_id}/tree/main"
            resp = requests.get(list_url, headers=headers)
            resp.raise_for_status()
            files_info = resp.json()
            csv_files = [f["path"] for f in files_info if f["path"].endswith('.csv')]

            if not csv_files:
                print("⚠️ 没有找到 CSV 文件", flush=True)
                logger.warning("没有找到 CSV 文件")
                self._release_lock()
                return False

            temp_dir = tempfile.mkdtemp()
            dataframes = {}

            for filename in csv_files:
                print(f"   ⬇️ 下载 {filename}...", flush=True)
                file_url = f"{base_url}/{filename}"
                resp = requests.get(file_url, headers=headers)
                resp.raise_for_status()

                table_name = filename[:-4]
                with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
                    f.write(resp.text)
                    temp_path = f.name

                df = pd.read_csv(temp_path, encoding='utf-8')
                os.unlink(temp_path)
                dataframes[table_name] = df
                logger.info(f"拉取表 {table_name}: {len(df)} 条记录")
                logger.info(f"  id列前5个值: {df['id'].head().tolist() if 'id' in df.columns else 'N/A'}")
                print(f"   - {table_name}: {len(df)} 条记录", flush=True)
            
            if not dataframes:
                print("⚠️ 没有找到 CSV 文件", flush=True)
                logger.warning("没有找到 CSV 文件")
                self._release_lock()
                return False
            
            # 3. 保存到本地数据库
            print("💾 保存到本地数据库...", flush=True)
            self._save_dataframes_to_sqlite(dataframes)

            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"\n✅ 成功从 Hugging Face Dataset 拉取: {self.repo_id}", flush=True)
            print(f"{'='*60}\n", flush=True)
            logger.info(f"✅ 成功从 Hugging Face Dataset 拉取: {self.repo_id}")
            self._release_lock()
            return True

        except Exception as e:
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"\n⚠️ 从 Hugging Face 拉取失败: {e}", flush=True)
            print(f"   💡 提示：这可能是首次运行，数据集还没有数据", flush=True)
            print(f"{'='*60}\n", flush=True)
            logger.warning(f"从 Hugging Face 拉取失败: {e}")
            logger.info("这可能是首次运行，数据集还没有数据")
            self._release_lock()
            return False
    
    def sync_to_hub(self, interval_minutes: int = 60):
        """
        定期同步到 Hugging Face Dataset（后台线程）
        
        Args:
            interval_minutes: 同步间隔（分钟）
        """
        import time
        import threading
        
        def sync_loop():
            while True:
                try:
                    self.push_to_hub(f"Auto-sync at {datetime.now().isoformat()}")
                except Exception as e:
                    logger.error(f"自动同步失败: {e}")
                
                time.sleep(interval_minutes * 60)
        
        thread = threading.Thread(target=sync_loop, daemon=True)
        thread.start()
        logger.info(f"已启动自动同步线程，间隔 {interval_minutes} 分钟")


def create_persistence_manager(
    repo_id: str = None,
    hf_token: str = None,
    auto_pull: bool = True
) -> Optional[HuggingFacePersistence]:
    """
    创建持久化管理器（从环境变量或参数读取配置）
    
    Args:
        repo_id: Hugging Face Dataset 仓库 ID
        hf_token: Hugging Face 访问令牌
        auto_pull: 是否自动从 Hub 拉取数据
        
    Returns:
        持久化管理器实例，如果配置不完整则返回 None
    """
    # 从环境变量读取
    repo_id = repo_id or os.environ.get("HF_DATASET_REPO_ID")
    hf_token = hf_token or os.environ.get("HF_TOKEN")
    
    if not repo_id or not hf_token:
        logger.info("Hugging Face 持久化未配置（缺少 HF_DATASET_REPO_ID 或 HF_TOKEN）")
        return None
    
    try:
        manager = HuggingFacePersistence(repo_id, hf_token)
        
        if auto_pull:
            logger.info("首次启动，从 Hugging Face 拉取数据...")
            manager.pull_from_hub()
        
        return manager
        
    except Exception as e:
        logger.error(f"创建持久化管理器失败: {e}")
        return None


if __name__ == "__main__":
    """测试持久化模块"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Hugging Face 持久化工具")
    parser.add_argument("--repo", required=True, help="Hugging Face Dataset 仓库 ID (用户名/数据集名)")
    parser.add_argument("--token", required=True, help="Hugging Face 访问令牌")
    parser.add_argument("--action", choices=["push", "pull"], required=True, help="操作: push 或 pull")
    parser.add_argument("--db", help="SQLite 数据库文件路径")
    
    args = parser.parse_args()
    
    manager = HuggingFacePersistence(
        repo_id=args.repo,
        hf_token=args.token,
        db_path=args.db
    )
    
    if args.action == "push":
        success = manager.push_to_hub("Manual push from CLI")
        sys.exit(0 if success else 1)
    elif args.action == "pull":
        success = manager.pull_from_hub()
        sys.exit(0 if success else 1)
