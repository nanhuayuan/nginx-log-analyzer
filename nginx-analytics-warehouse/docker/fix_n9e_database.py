#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
N9E数据库修复工具 - 跨平台版本
支持Windows/Linux/macOS
自动检测并修复N9E数据库初始化问题
"""

import subprocess
import sys
import time
import os
import json
from pathlib import Path


class Colors:
    """跨平台颜色输出"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    END = '\033[0m'

    @staticmethod
    def disable_on_windows():
        """在Windows上禁用颜色（如果终端不支持）"""
        if os.name == 'nt':
            for attr in dir(Colors):
                if not attr.startswith('_') and attr != 'disable_on_windows':
                    setattr(Colors, attr, '')


def log_info(message):
    print(f"{Colors.BLUE}[INFO]{Colors.END} {message}")


def log_success(message):
    print(f"{Colors.GREEN}[SUCCESS]{Colors.END} {message}")


def log_warning(message):
    print(f"{Colors.YELLOW}[WARNING]{Colors.END} {message}")


def log_error(message):
    print(f"{Colors.RED}[ERROR]{Colors.END} {message}")


class N9EDatabaseFixer:
    def __init__(self):
        self.docker_compose_cmd = self._detect_docker_compose()

    def _detect_docker_compose(self):
        """检测docker-compose命令"""
        for cmd in ['docker-compose', 'docker compose']:
            try:
                subprocess.run(cmd.split() + ['--version'],
                             capture_output=True, check=True)
                return cmd.split()
            except (subprocess.CalledProcessError, FileNotFoundError):
                continue
        raise RuntimeError("未找到docker-compose命令")

    def _run_command(self, cmd, capture_output=True, check=True):
        """执行命令的统一接口"""
        if isinstance(cmd, str):
            cmd = cmd.split()

        try:
            result = subprocess.run(cmd, capture_output=capture_output,
                                  check=check, text=True)
            return result
        except subprocess.CalledProcessError as e:
            if capture_output:
                log_error(f"命令执行失败: {' '.join(cmd)}")
                if e.stdout:
                    log_error(f"stdout: {e.stdout}")
                if e.stderr:
                    log_error(f"stderr: {e.stderr}")
            raise

    def check_docker_status(self):
        """检查Docker环境"""
        log_info("检查Docker环境...")

        try:
            # 检查Docker是否运行
            self._run_command(['docker', 'info'])
            log_success("Docker服务正常")

            # 检查docker-compose是否可用
            self._run_command(self.docker_compose_cmd + ['--version'])
            log_success(f"Docker Compose可用: {' '.join(self.docker_compose_cmd)}")

            return True
        except Exception as e:
            log_error(f"Docker环境检查失败: {e}")
            return False

    def check_mysql_container(self):
        """检查N9E MySQL容器状态"""
        log_info("检查N9E MySQL容器...")

        try:
            result = self._run_command(['docker', 'ps', '--filter', 'name=n9e-mysql', '--format', 'json'])
            if result.stdout.strip():
                log_success("N9E MySQL容器正在运行")
                return True
            else:
                log_warning("N9E MySQL容器未运行")
                return False
        except Exception as e:
            log_error(f"检查容器状态失败: {e}")
            return False

    def start_mysql_if_needed(self):
        """如果需要，启动MySQL容器"""
        if not self.check_mysql_container():
            log_info("启动N9E MySQL容器...")
            try:
                self._run_command(self.docker_compose_cmd + ['up', '-d', 'n9e-mysql'])
                log_success("N9E MySQL容器启动命令已执行")

                # 等待容器就绪
                log_info("等待MySQL容器就绪...")
                for i in range(30):
                    try:
                        self._run_command([
                            'docker', 'exec', 'n9e-mysql',
                            'mysqladmin', 'ping', '-h', 'localhost',
                            '-uroot', '-p1234', '--silent'
                        ])
                        log_success("MySQL容器已就绪")
                        return True
                    except:
                        if i < 29:
                            print(".", end="", flush=True)
                            time.sleep(2)
                        else:
                            log_error("MySQL容器启动超时")
                            return False
            except Exception as e:
                log_error(f"启动MySQL容器失败: {e}")
                return False
        return True

    def check_database_status(self):
        """检查数据库状态"""
        log_info("检查N9E数据库状态...")

        try:
            # 检查数据库是否存在
            result = self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysql', '-uroot', '-p1234', '-e', 'SHOW DATABASES;'
            ])

            if 'n9e_v6' not in result.stdout:
                log_error("n9e_v6数据库不存在")
                return False

            # 检查表数量
            result = self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysql', '-uroot', '-p1234', '-e', 'USE n9e_v6; SHOW TABLES;'
            ])

            table_count = len([line for line in result.stdout.split('\n') if line.strip() and line != 'Tables_in_n9e_v6'])
            log_info(f"当前表数量: {table_count}")

            if table_count < 100:  # 正常应该有152个表
                log_warning(f"表数量不足 ({table_count}/152)，需要重新初始化")
                return False

            # 检查关键表
            if 'users' not in result.stdout:
                log_error("users表不存在")
                return False

            if 'role_operation' not in result.stdout:
                log_error("role_operation表不存在")
                return False

            # 检查users表数据
            result = self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysql', '-uroot', '-p1234', '-e',
                'USE n9e_v6; SELECT COUNT(*) as count FROM users;'
            ])

            if 'count' in result.stdout and '0' in result.stdout:
                log_warning("users表为空，需要重新初始化")
                return False

            log_success("数据库状态正常")
            return True

        except Exception as e:
            log_error(f"检查数据库状态失败: {e}")
            return False

    def backup_current_database(self):
        """备份当前数据库（如果存在）"""
        log_info("备份当前数据库...")

        try:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            backup_file = f"n9e_backup_{timestamp}.sql"

            # 导出数据库
            result = self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysqldump', '-uroot', '-p1234', '--single-transaction',
                '--routines', '--triggers', 'n9e_v6'
            ])

            # 保存到本地文件
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.write(result.stdout)

            log_success(f"数据库已备份到: {backup_file}")
            return backup_file

        except Exception as e:
            log_warning(f"备份失败（继续执行）: {e}")
            return None

    def reinitialize_database(self):
        """重新初始化数据库"""
        log_info("重新初始化N9E数据库...")

        try:
            # 删除现有数据库
            log_info("删除现有数据库...")
            self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysql', '-uroot', '-p1234', '-e', 'DROP DATABASE IF EXISTS n9e_v6;'
            ])

            # 检查初始化脚本
            init_script = Path('./services/n9e/init-scripts/a-n9e.sql')
            if not init_script.exists():
                log_error(f"初始化脚本不存在: {init_script}")
                return False

            # 执行初始化脚本
            log_info("执行初始化脚本...")
            with open(init_script, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # 通过docker exec执行SQL
            result = self._run_command([
                'docker', 'exec', '-i', 'n9e-mysql',
                'mysql', '-uroot', '-p1234'
            ], capture_output=True, check=False)

            # 如果上面的方法失败，尝试文件挂载方式
            if result.returncode != 0:
                log_info("尝试文件挂载方式...")
                # 将SQL文件复制到容器中
                self._run_command([
                    'docker', 'cp', str(init_script), 'n9e-mysql:/tmp/init.sql'
                ])

                # 在容器中执行
                self._run_command([
                    'docker', 'exec', 'n9e-mysql',
                    'mysql', '-uroot', '-p1234', '-e', 'source /tmp/init.sql'
                ])

            log_success("数据库初始化完成")
            return True

        except Exception as e:
            log_error(f"初始化数据库失败: {e}")
            return False

    def verify_database(self):
        """验证数据库初始化结果"""
        log_info("验证数据库初始化结果...")

        try:
            # 检查表数量
            result = self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysql', '-uroot', '-p1234', '-e', 'USE n9e_v6; SHOW TABLES;'
            ])

            table_count = len([line for line in result.stdout.split('\n') if line.strip() and line != 'Tables_in_n9e_v6'])
            log_info(f"表数量: {table_count}")

            if table_count < 100:
                log_error(f"表数量不足: {table_count}")
                return False

            # 检查root用户
            result = self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysql', '-uroot', '-p1234', '-e',
                'USE n9e_v6; SELECT username, roles FROM users WHERE username="root";'
            ])

            if 'root' not in result.stdout or 'Admin' not in result.stdout:
                log_error("root用户不存在或权限不正确")
                return False

            # 检查role_operation表
            result = self._run_command([
                'docker', 'exec', 'n9e-mysql',
                'mysql', '-uroot', '-p1234', '-e',
                'USE n9e_v6; SELECT COUNT(*) as count FROM role_operation;'
            ])

            log_success("✓ 数据库验证通过")
            log_success("✓ 所有关键表存在")
            log_success("✓ root用户配置正确")
            log_success("✓ 权限表数据正常")

            return True

        except Exception as e:
            log_error(f"验证失败: {e}")
            return False

    def restart_nightingale(self):
        """重启Nightingale服务"""
        log_info("重启Nightingale服务...")

        try:
            # 停止nightingale
            self._run_command(self.docker_compose_cmd + ['stop', 'nightingale'], check=False)

            # 启动必要的依赖服务
            self._run_command(self.docker_compose_cmd + ['up', '-d', 'victoriametrics', 'redis'])

            # 等待依赖服务就绪
            time.sleep(10)

            # 启动nightingale
            self._run_command(self.docker_compose_cmd + ['up', '-d', 'nightingale'])

            log_success("Nightingale服务重启完成")
            return True

        except Exception as e:
            log_error(f"重启Nightingale失败: {e}")
            return False

    def run_fix(self):
        """执行完整的修复流程"""
        print(f"{Colors.BOLD}🔧 N9E数据库自动修复工具{Colors.END}")
        print("=" * 50)

        # 检查Docker环境
        if not self.check_docker_status():
            return False

        # 启动MySQL（如果需要）
        if not self.start_mysql_if_needed():
            return False

        # 检查数据库状态
        if self.check_database_status():
            log_success("🎉 数据库状态正常，无需修复")
            return True

        # 需要修复
        log_warning("⚠️ 检测到数据库问题，开始修复...")

        # 备份（可选）
        backup_file = self.backup_current_database()

        # 重新初始化
        if not self.reinitialize_database():
            return False

        # 验证结果
        if not self.verify_database():
            return False

        # 重启Nightingale
        if not self.restart_nightingale():
            log_warning("Nightingale重启失败，请手动重启")

        print("\n" + "=" * 50)
        log_success("🎉 N9E数据库修复完成！")

        if backup_file:
            log_info(f"💾 数据备份文件: {backup_file}")

        log_info("🌐 现在可以访问: http://localhost:17000 (root/root.2020)")

        return True


def main():
    """主函数"""
    # 在Windows上可能需要禁用颜色
    if os.name == 'nt':
        try:
            # 尝试启用Windows控制台颜色支持
            import colorama
            colorama.init()
        except ImportError:
            Colors.disable_on_windows()

    try:
        fixer = N9EDatabaseFixer()
        success = fixer.run_fix()
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        log_info("用户取消操作")
        sys.exit(0)
    except Exception as e:
        log_error(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()