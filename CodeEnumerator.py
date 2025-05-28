import os
import pathspec

class CodeEnumerator:
    def __init__(self, root_dir, additional_types=[]):
        self.root_dir = root_dir
        self.extensions = ['.py'] + additional_types
        self.ext_to_lang = {'.py': 'python', '.csv': 'csv', '.json': 'json', '.txt': 'text', '.html': 'html', '.css': 'css', '.md': 'markdown'}
        self.gitignore_spec = self._load_gitignore()

    def _load_gitignore(self):
        gitignore_path = os.path.join(self.root_dir, '.gitignore')
        if os.path.exists(gitignore_path):
            with open(gitignore_path, 'r') as f:
                return pathspec.PathSpec.from_lines('gitwildmatch', f)
        return None

    def _is_ignored(self, path):
        if self.gitignore_spec is None:
            return False
        rel_path = os.path.relpath(path, self.root_dir)
        return self.gitignore_spec.match_file(rel_path)

    def get_relevant_files(self):
        files = []
        for dirpath, _, filenames in os.walk(self.root_dir):
            if self._is_ignored(dirpath):
                continue
            for f in filenames:
                file_path = os.path.join(dirpath, f)
                if not self._is_ignored(file_path) and any(f.endswith(ext) for ext in self.extensions):
                    files.append(file_path)
        return files

    def build_tree(self):
        tree = {}
        for file_path in self.get_relevant_files():
            rel_path = os.path.relpath(file_path, self.root_dir)
            parts = rel_path.split(os.sep)
            current = tree
            for part in parts[:-1]:
                current = current.setdefault(part, {})
            current[parts[-1]] = None
        return tree

    def print_tree(self, node, indent=0):
        lines = []
        for key, value in sorted(node.items()):
            lines.append('  ' * indent + '|-- ' + (key + '/' if value else key))
            if value:
                lines.extend(self.print_tree(value, indent + 1))
        return lines

    def get_code_blocks(self):
        blocks = []
        for file_path in self.get_relevant_files():
            rel_path = os.path.relpath(file_path, self.root_dir)
            ext = os.path.splitext(file_path)[1]
            lang = self.ext_to_lang.get(ext, 'text')
            with open(file_path, 'r') as f:
                content = f.read()
            blocks.append(f"{rel_path}\n```{lang}\n{content}\n```")
        return blocks

    def generate_output(self):
        tree = "Folder Tree:\n" + "\n".join(self.print_tree(self.build_tree()))
        with open('header.txt', 'r') as f:
            header = f.read()
        code_blocks = "\n\n".join(self.get_code_blocks())
        with open('footer.txt', 'r') as f:
            footer = f.read()
        with open('additional.txt', 'r') as f:
            additional = f"'{f.read()}'"
        return f"{tree}\n\n{header}\n\n{code_blocks}\n\n{footer}\n\n{additional}"

if __name__ == "__main__":
    # Specify the root directory to scan
    root_directory = "."  # Current directory; change to desired path
    # Include additional file types
    additional_types = ['.csv', '.json', '.txt', '.html', '.css', '.md']
    
    # Create CodeEnumerator instance
    enumerator = CodeEnumerator(root_directory, additional_types)
    
    # Generate and print the output
    output = enumerator.generate_output()
    print(output)
    
    # Optionally, save to a file
    with open('code_summary.md', 'w') as f:
        f.write(output)