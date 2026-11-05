"""Offline checks for centralized, server-safe object-oriented schema ownership."""

import ast
from pathlib import Path
import re
import unittest
import sqlstructure
from DataBaseCore import DataBaseConnectionCore
from DataBaseCore import statement_timeout_message
import os
import sys


ROOT = Path(__file__).resolve().parent


class TestSQLStructure(unittest.TestCase):
    def test_schema_and_timeout_methods_belong_to_classes(self):
        tree = ast.parse((ROOT / 'sqlstructure.py').read_text())
        self.assertFalse(any(isinstance(node, ast.FunctionDef) for node in tree.body))
        self.assertTrue(issubclass(DataBaseConnectionCore, sqlstructure.SQLStructure))
        self.assertTrue(issubclass(sqlstructure.SQLStructureCLI, sqlstructure.SQLStructure))
        self.assertIs(statement_timeout_message, sqlstructure.SQLStructure.statement_timeout_message)
        for name in ('init_database', 'init_alldirs', 'create_hosts_table',
                     'create_job_work_table', 'ensure_jobs_constraints', 'check_schema',
                     '_show_statement_timeout_hint'):
            self.assertEqual(getattr(sqlstructure.SQLStructureCLI, name).__module__, 'sqlstructure')

    def test_current_runtime_has_no_schema_sql_outside_sqlstructure(self):
        pattern = re.compile(
            r'CREATE\s+(?:TEMPORARY\s+)?(?:TABLE|DATABASE|INDEX)|ALTER\s+TABLE|'
            r'information_schema\.(?:tables|columns|statistics)|SHOW\s+COLUMNS|'
            r'(?:SELECT|SET)[^;]*max_statement_time', re.I | re.S)
        for filename in ('DataBaseCore.py', 'DataBaseCLI.py', 'version25.py', 'dirwalker.py'):
            with self.subTest(file=filename):
                tree = ast.parse((ROOT / filename).read_text())
                for node in ast.walk(tree):
                    if isinstance(node, ast.Constant) and isinstance(node.value, str):
                        self.assertIsNone(pattern.search(node.value),
                                          f'{filename}:{node.lineno} contains schema SQL')

    def test_schema_import_does_not_load_application_or_client(self):
        tree = ast.parse((ROOT / 'sqlstructure.py').read_text())
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imports.append(node.module)
        self.assertEqual(imports, ['re'])


# release regression coverage.


ROOT = Path(__file__).resolve().parent
RELEASE_SOURCE = os.environ.get("RELEASE_SOURCE") or max(
    ROOT.glob("version[0-9]*.py"),
    key=lambda path: int(path.stem[len("version"):]),
).name
PRODUCTION_FILES = (
    RELEASE_SOURCE,
    "DataBaseCore.py",
    "sqlstructure.py",
    "DataBaseCLI.py",
    "client_operations.py",
    "server_operations.py",
    "doubles_output.py",
)


def table_creation_locations(source):
    """Find CREATE TABLE SQL, including f-strings and concatenated fragments."""
    def sql_text(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            return ''.join(sql_text(value) or ' ' for value in node.values)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return (sql_text(node.left) or ' ') + (sql_text(node.right) or ' ')
        return None

    locations = set()
    for node in ast.walk(ast.parse(source)):
        text = sql_text(node)
        if text is None:
            continue
        text = re.sub(r'/\*.*?\*/|--[^\n]*', ' ', text, flags=re.S)
        if re.search(r'\bCREATE\s+(?:TEMPORARY\s+)?TABLE\b', text, re.I):
            locations.add(node.lineno)
    return sorted(locations)


class TestTableCreationOwnership(unittest.TestCase):
    def test_only_sqlstructure_contains_create_table(self):
        self.assertIn('sqlstructure.py', PRODUCTION_FILES)
        for name in dict.fromkeys(PRODUCTION_FILES + ('dirwalker.py',)):
            locations = table_creation_locations((ROOT / name).read_text(encoding='utf-8'))
            with self.subTest(file=name):
                if name == 'sqlstructure.py':
                    self.assertTrue(locations, 'The schema module must contain table definitions')
                else:
                    self.assertFalse(locations,
                                     f'{name}:{locations}: CREATE TABLE belongs in sqlstructure.py')

    def test_detects_split_multiline_temporary_and_formatted_sql(self):
        examples = [
            'cursor.execute("CREATE TABLE example (id INT)")',
            'cursor.execute(f"create\\n temporary\\n table {name} (id INT)")',
            'query = "CREATE " + "TABLE example (id INT)"',
            'cursor.execute("CREATE /* schema */ TABLE example (id INT)")',
        ]
        for source in examples:
            with self.subTest(source=source):
                self.assertTrue(table_creation_locations(source))
        self.assertFalse(table_creation_locations('cursor.execute("SELECT * FROM example")'))


class TestPythonVersion(unittest.TestCase):
    def test_current_test_files_match_the_seven_production_modules(self):
        expected = {'test_' + Path(name).stem + '.py' for name in PRODUCTION_FILES}
        self.assertEqual({path.name for path in ROOT.glob('test_*.py')}, expected)
        self.assertFalse(list((ROOT / 'archive').glob('test_version*.py')))

    def test_python_38(self):
        self.assertEqual(sys.version_info[:2], (3, 8), "Release requires Python 3.8")

    def test_production_syntax(self):
        for name in PRODUCTION_FILES:
            with self.subTest(file=name):
                ast.parse((ROOT / name).read_text(encoding="utf-8"), filename=name,
                          feature_version=8)


class TestProductionVersions(unittest.TestCase):
    def test_matching_release_versions(self):
        source = (ROOT / RELEASE_SOURCE).read_text(encoding="utf-8")
        release = re.search(r"^# version (\d+\.\d+\.\d+)\s*$", source, re.MULTILINE)
        self.assertIsNotNone(release, "Release source needs a version header")
        expected = release.group(1)
        self.assertEqual(int(expected.split(".")[1]),
                         int(Path(RELEASE_SOURCE).stem[len("version"):]))
        for name in PRODUCTION_FILES[1:]:
            with self.subTest(file=name):
                tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))
                versions = [ast.literal_eval(node.value) for node in tree.body
                            if isinstance(node, ast.Assign)
                            and any(isinstance(target, ast.Name)
                                    and target.id == "__version__" for target in node.targets)]
                self.assertEqual(versions, [expected])


if __name__ == '__main__':
    unittest.main()
