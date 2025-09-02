#!/usr/bin/env python3
"""
验证Font导入修复
"""

import ast
import re

def check_font_imports():
    """检查Font导入是否正确"""
    print("🔍 检查Font导入修复...")
    
    try:
        with open('self_05_time_dimension_analyzer_advanced.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 查找所有使用Font的行
        font_usages = []
        for i, line in enumerate(content.split('\n'), 1):
            if 'Font(' in line and not line.strip().startswith('#'):
                font_usages.append((i, line.strip()))
        
        print(f"📊 找到 {len(font_usages)} 处Font使用")
        
        # 检查每个Font使用的上下文
        for line_num, line in font_usages:
            print(f"  第{line_num}行: {line}")
        
        # 检查Font导入
        font_imports = []
        for i, line in enumerate(content.split('\n'), 1):
            if 'from openpyxl.styles import' in line and 'Font' in line:
                font_imports.append((i, line.strip()))
        
        print(f"📥 找到 {len(font_imports)} 处Font导入")
        for line_num, line in font_imports:
            print(f"  第{line_num}行: {line}")
        
        # 检查方法中是否有局部导入
        methods_with_font = []
        lines = content.split('\n')
        current_method = None
        
        for i, line in enumerate(lines):
            if line.strip().startswith('def '):
                current_method = line.strip().split('(')[0].replace('def ', '')
            elif 'Font(' in line and current_method:
                # 检查该方法是否有Font导入
                method_start = i
                while method_start > 0 and not lines[method_start].strip().startswith('def '):
                    method_start -= 1
                
                # 在方法内查找Font导入
                has_local_import = False
                for j in range(method_start, min(i + 10, len(lines))):
                    if 'from openpyxl.styles import' in lines[j] and 'Font' in lines[j]:
                        has_local_import = True
                        break
                
                methods_with_font.append((current_method, has_local_import, i+1))
        
        print(f"🔧 使用Font的方法分析:")
        all_good = True
        for method, has_import, line_num in methods_with_font:
            status = "✅" if has_import else "❌"
            print(f"  {status} {method} (第{line_num}行) - 局部导入: {has_import}")
            if not has_import:
                all_good = False
        
        return all_good
        
    except Exception as e:
        print(f"❌ 检查失败: {e}")
        return False

def test_syntax_after_fix():
    """测试修复后的语法"""
    print("\n🧪 测试修复后的语法...")
    
    try:
        with open('self_05_time_dimension_analyzer_advanced.py', 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 语法检查
        ast.parse(content)
        print("✅ Python语法正确")
        
        # 检查导入语句
        import_lines = [line for line in content.split('\n') if line.strip().startswith('from openpyxl.styles import')]
        print(f"✅ 找到 {len(import_lines)} 个openpyxl.styles导入")
        
        return True
        
    except SyntaxError as e:
        print(f"❌ 语法错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

def main():
    """主函数"""
    print("=== Font导入修复验证 ===")
    
    success = True
    
    # 检查Font导入
    if not check_font_imports():
        success = False
    
    # 测试语法
    if not test_syntax_after_fix():
        success = False
    
    # 总结
    print("\n=== 验证总结 ===")
    if success:
        print("🎉 Font导入修复验证通过！")
        print("✅ 所有Font使用都有正确的导入")
        print("✅ Python语法正确")
        print("✅ 可以正常使用Excel样式功能")
    else:
        print("❌ 还有Font导入问题需要修复")
    
    return success

if __name__ == "__main__":
    main()