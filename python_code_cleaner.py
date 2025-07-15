#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import sys
import argparse
from typing import List, Tuple


class PythonCodeCleaner:
    """Python代码清理工具，用于移除print语句、注释和log_info调用等内容"""

    def __init__(self):
        # 正则表达式定义
        self.patterns = [
            # 移除print语句 - 支持多种print格式
            #re.compile(r'^\s*print\s*\(.*?\).*?(\r?\n|$)', re.MULTILINE),
            # 支持Python 2风格的print语句
            #re.compile(r'^\s*print\s+[^(].*?(\r?\n|$)', re.MULTILINE),
            # 移除行尾注释（确保不会误删除字符串中的#）
            re.compile(r'(?<!\")(?<!\')(?<=\S)\s*#.*?$', re.MULTILINE),
            # 移除独立的单行注释
            re.compile(r'^\s*#.*?$', re.MULTILINE),
            # 移除三引号文档字符串（单行或多行）- 优化处理嵌套引号的情况
            re.compile(r'^\s*("""|\'\'\')(?s:(?!\1).)*?\1\s*(\r?\n|$)', re.MULTILINE),
            # 移除log_info函数调用，但保留函数定义
            #re.compile(r'(?<!def\s)log_info\s*\([^)]*\)\s*(\r?\n|$)', re.MULTILINE)
        ]

    def clean_code(self, content: str, iterations: int = 3) -> str:
        """
        清理Python代码内容

        Args:
            content (str): 原始Python代码内容
            iterations (int): 重复清理的次数，确保彻底移除所有匹配内容

        Returns:
            str: 清理后的Python代码
        """
        cleaned_content = content

        # 多次迭代清理，确保完全移除所有需要清理的内容
        for _ in range(iterations):
            for pattern in self.patterns:
                cleaned_content = pattern.sub('', cleaned_content)

            # 清理连续的空行
            cleaned_content = re.sub(r'\n\s*\n+', '\n\n', cleaned_content)

        # 确保文件末尾只有一个换行符
        cleaned_content = cleaned_content.rstrip() + '\n'

        return cleaned_content

    def process_file(self, input_file: str, output_file: str = None, iterations: int = 3) -> str:
        """
        处理单个Python文件

        Args:
            input_file (str): 输入文件路径
            output_file (str, optional): 输出文件路径。如果为None，则生成默认输出路径
            iterations (int): 重复清理的次数

        Returns:
            str: 输出文件路径
        """
        # 确保输入文件存在
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"输入文件不存在: {input_file}")

        # 如果没有指定输出文件，则生成默认输出文件名
        if output_file is None:
            filename, ext = os.path.splitext(input_file)
            output_file = f"{filename}_cleaned{ext}"

        try:
            # 读取文件内容
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # 清理代码
            cleaned_content = self.clean_code(content, iterations)

            # 写入清理后的内容到输出文件
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(cleaned_content)

            return output_file
        except UnicodeDecodeError:
            # 如果UTF-8解码失败，尝试其他编码
            encodings = ['gbk', 'latin-1', 'cp1252']
            for encoding in encodings:
                try:
                    with open(input_file, 'r', encoding=encoding) as f:
                        content = f.read()
                    cleaned_content = self.clean_code(content, iterations)
                    with open(output_file, 'w', encoding=encoding) as f:
                        f.write(cleaned_content)
                    return output_file
                except UnicodeDecodeError:
                    continue

            # 所有编码尝试失败
            raise UnicodeDecodeError(f"无法解码文件: {input_file}，尝试了以下编码: utf-8, {', '.join(encodings)}")

    def process_directory(self, input_dir: str, output_dir: str = None,
                          recursive: bool = True, iterations: int = 3) -> List[str]:
        """
        批量处理目录中的Python文件

        Args:
            input_dir (str): 输入目录路径
            output_dir (str, optional): 输出目录路径。如果为None，则使用"input_dir_cleaned"
            recursive (bool): 是否递归处理子目录
            iterations (int): 清理迭代次数

        Returns:
            List[str]: 处理后的文件路径列表
        """
        # 确保输入目录存在
        if not os.path.exists(input_dir):
            raise FileNotFoundError(f"输入目录不存在: {input_dir}")

        # 处理输出目录
        if output_dir is None:
            # 默认输出到原目录的同级目录下，添加"_cleaned"后缀
            parent_dir = os.path.dirname(input_dir.rstrip(os.sep))
            dir_name = os.path.basename(input_dir.rstrip(os.sep))
            output_dir = os.path.join(parent_dir, f"{dir_name}_cleaned")

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)

        processed_files = []
        failed_files = []

        # 遍历目录
        if recursive:
            # 递归处理所有子目录
            for root, dirs, files in os.walk(input_dir):
                # 计算相对路径，用于在输出目录中创建相同的目录结构
                rel_path = os.path.relpath(root, input_dir)
                if rel_path == '.':
                    # 当前处理的是根目录
                    target_dir = output_dir
                else:
                    # 当前处理的是子目录
                    target_dir = os.path.join(output_dir, rel_path)

                # 确保目标目录存在
                os.makedirs(target_dir, exist_ok=True)

                # 处理当前目录中的所有Python文件
                for file in files:
                    if file.endswith('.py'):
                        input_file = os.path.join(root, file)
                        output_file = os.path.join(target_dir, file)

                        try:
                            self.process_file(input_file, output_file, iterations)
                            processed_files.append((input_file, output_file))
                        except Exception as e:
                            failed_files.append((input_file, str(e)))
        else:
            # 只处理当前目录，不递归
            for file in os.listdir(input_dir):
                if file.endswith('.py'):
                    input_file = os.path.join(input_dir, file)
                    output_file = os.path.join(output_dir, file)

                    try:
                        self.process_file(input_file, output_file, iterations)
                        processed_files.append((input_file, output_file))
                    except Exception as e:
                        failed_files.append((input_file, str(e)))

        # 返回处理结果
        return processed_files, failed_files


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Python代码清理工具 - 移除print语句、注释和log_info调用等内容')

    # 输入输出参数组
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('-f', '--file', help='输入Python文件路径')
    input_group.add_argument('-d', '--directory', help='输入目录路径，将处理其中所有Python文件')

    # 文件输出选项
    parser.add_argument('-o', '--output', help='输出文件或目录路径（可选）')

    # 目录处理选项
    parser.add_argument('-r', '--recursive', action='store_true', default=True,
                        help='递归处理子目录（默认: True）')
    parser.add_argument('--no-recursive', action='store_false', dest='recursive',
                        help='不递归处理子目录')

    # 清理选项
    parser.add_argument('-i', '--iterations', type=int, default=3,
                        help='重复清理的次数（默认: 3）')

    # 自定义规则选项
    parser.add_argument('--disable-print', action='store_true',
                        help='禁用移除print语句')
    parser.add_argument('--disable-comments', action='store_true',
                        help='禁用移除注释')
    parser.add_argument('--disable-docstrings', action='store_true',
                        help='禁用移除文档字符串')
    parser.add_argument('--disable-logs', action='store_true',
                        help='禁用移除log_info调用')

    return parser.parse_args()


def main():
    """主函数"""
    try:
        # 判断是否有命令行参数
        if len(sys.argv) > 1:
            # 使用命令行参数
            args = parse_args()
            cleaner = PythonCodeCleaner()

            # 自定义规则处理（根据命令行参数禁用特定规则）
            if args.disable_print:
                cleaner.patterns = [p for p in cleaner.patterns if not str(p.pattern).startswith(r'^\s*print')]
            if args.disable_comments:
                cleaner.patterns = [p for p in cleaner.patterns if not (r'#.*?$' in str(p.pattern))]
            if args.disable_docstrings:
                cleaner.patterns = [p for p in cleaner.patterns if not (r'"""|\'\'\'' in str(p.pattern))]
            if args.disable_logs:
                cleaner.patterns = [p for p in cleaner.patterns if not (r'log_info' in str(p.pattern))]

            # 处理文件或目录
            if args.file:
                # 处理单个文件
                output_file = cleaner.process_file(args.file, args.output, args.iterations)
                print(f"处理完成！输出文件: {output_file}")
            else:
                # 处理目录
                processed_files, failed_files = cleaner.process_directory(
                    args.directory, args.output, args.recursive, args.iterations
                )

                # 打印处理结果
                print(f"目录处理完成！共处理 {len(processed_files)} 个文件。")
                if failed_files:
                    print(f"失败: {len(failed_files)} 个文件:")
                    for input_file, error in failed_files:
                        print(f"  - {input_file}: {error}")
        else:
            # 交互式菜单
            print("Python代码清理工具")
            print("=" * 40)
            print("1. 清理单个Python文件")
            print("2. 清理整个目录")
            print("3. 退出")
            print("=" * 40)

            choice = input("请选择操作 [1-3]: ")

            if choice == '1':
                input_file = input("请输入Python文件路径: ")
                output_file = input("请输入输出文件路径 (留空为默认): ").strip() or None
                iterations = int(input("请输入清理迭代次数 [默认:3]: ").strip() or "3")

                cleaner = PythonCodeCleaner()
                output_file = cleaner.process_file(input_file, output_file, iterations)
                print(f"清理完成！输出文件: {output_file}")

            elif choice == '2':
                input_dir = input("请输入目录路径: ")
                output_dir = input("请输入输出目录路径 (留空为默认): ").strip() or None
                recursive_choice = input("是否递归处理子目录? (y/n) [y]: ").strip().lower() or "y"
                recursive = recursive_choice in ('y', 'yes', 'true', '1')
                iterations = int(input("请输入清理迭代次数 [默认:3]: ").strip() or "3")

                cleaner = PythonCodeCleaner()
                processed_files, failed_files = cleaner.process_directory(
                    input_dir, output_dir, recursive, iterations
                )

                print(f"目录处理完成！共处理 {len(processed_files)} 个文件。")
                if failed_files:
                    print(f"失败: {len(failed_files)} 个文件:")
                    for input_file, error in failed_files:
                        print(f"  - {input_file}: {error}")
            else:
                print("退出程序")
                sys.exit(0)

    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
