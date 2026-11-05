"""Tests for doubles_output.py."""

from unittest import mock
import DataBaseCore as core
# Importing a test module must not probe the configured production database.
with mock.patch.object(core.DataBaseConnectionCore, 'connect', side_effect=RuntimeError('offline test')):
    import version25 as app
import contextlib
import io
import os
import tempfile
import unittest
from datetime import datetime
from types import SimpleNamespace
from client_operations import ClientArgumentParser
from pathlib import Path
from unittest.mock import patch
from client_operations import client_operations
from DataBaseCLI import DataBaseCLI
from doubles_output import doubles_output
import server_operations as server


class TestDoubles(unittest.TestCase):
    def test_doubles_progress_polls_until_closed_and_lists_duplicate_files(self):
        operation = app.client_operations(
            SimpleNamespace(mode="doubles-progress", jobid=34)
        )
        with tempfile.TemporaryDirectory() as temporary:
            left = os.path.join(temporary, "left")
            right = os.path.join(temporary, "right")
            os.makedirs(left)
            os.makedirs(right)
            for directory in (left, right):
                with open(os.path.join(directory, "image.bin"), "wb") as stream:
                    stream.write(b"x" * 1536)
            progress = [(34, "active", 2, 1), (34, "closed", 2, 2)]
            pairs = [
                (1, left, "a" * 64, "0", 2, right),
                (2, right, "a" * 64, "0", 1, left),
            ]
            with mock.patch.object(
                operation.database, "get_double_job_progress", side_effect=progress
            ) as get_progress, mock.patch.object(
                operation.database, "get_job",
                return_value={"jobid": 34, "hostid": 1, "last_activity": datetime(2026, 10, 28, 12, 34, 56)},
            ) as get_job, mock.patch.object(
                operation.database, "list_verified_double_pairs", return_value=pairs
            ), mock.patch.object(app.time, "sleep") as sleep, \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                operation.show_doubles_progress(34)

        self.assertEqual(get_progress.call_args_list, [mock.call(34), mock.call(34)])
        sleep.assert_called_once_with(20)
        self.assertIn("\x1b[2KJob 34: 50%", output.getvalue())
        self.assertIn("\x1b[2KJob 34: 100%", output.getvalue())
        self.assertIn(f"1 | {left} | image.bin | 1.50 KB | 2", output.getvalue())
        self.assertIn(f"1 | {right} | image.bin | 1.50 KB | 2", output.getvalue())

        get_job.assert_called_once_with(34)
        self.assertIn("Job 34: jobs.last_activity = 2026-10-28 12:34:56 UTC", output.getvalue())
        self.assertIn("Tabelle directories 1 verändert", output.getvalue())
        self.assertNotIn("<hostid>", output.getvalue())

    def test_doubles_progress_prompts_to_start_check_when_no_binarycheck_job_exists(self):
        operation = app.client_operations(
            SimpleNamespace(mode="doubles-progress", jobid=None)
        )
        with mock.patch.object(
            operation.database, "get_double_job_progress", return_value=None
        ) as get_progress, contextlib.redirect_stdout(io.StringIO()) as output:
            operation.show_doubles_progress()
        get_progress.assert_called_once_with(None)
        lines = output.getvalue().strip().splitlines()
        self.assertEqual(lines[-1], "need to start proess with --doubles-letcheck")

    def test_doubles_progress_html_writes_group_rows_with_path_tooltips(self):
        operation = app.client_operations(
            SimpleNamespace(mode="doubles-progress-html", jobid=34)
        )
        with tempfile.TemporaryDirectory() as temporary:
            left = os.path.join(temporary, "left")
            right = os.path.join(temporary, "right")
            os.makedirs(left)
            os.makedirs(right)
            for directory in (left, right):
                with open(os.path.join(directory, "photo.jpg"), "wb") as stream:
                    stream.write(b"photo")
            pairs = [(1, left, "a" * 64, "0", 2, right, 4096, 8192)]
            with mock.patch.object(
                operation.database, "get_double_job_progress",
                return_value=(34, "closed", 2, 2),
            ) as get_progress, mock.patch.object(
                operation.database, "get_double_job_batch", return_value=[(34, 0)]
            ), mock.patch.object(
                operation.database, "list_verified_double_pairs", return_value=pairs
            ) as list_pairs, mock.patch(
                "version22.Path.cwd", return_value=app.Path(temporary)
            ), contextlib.redirect_stdout(io.StringIO()) as output:
                operation.show_doubles_progress_html(34)

            page_path = os.path.join(temporary, "doubles-progress-34.html")
            with open(page_path, encoding="utf-8") as page:
                content = page.read()

        self.assertEqual(get_progress.call_args_list, [mock.call(34), mock.call(34)])
        self.assertEqual(list_pairs.call_args_list, [mock.call(34)])
        self.assertIn("doubles-progress-34.html", output.getvalue())
        self.assertNotIn('http-equiv="refresh" content="60"', content)
        self.assertIn("Binarycheck beendet. Dies ist der abschließende HTML-Stand.", content)
        self.assertIn("Größe des ersten Verzeichnisses", content)
        self.assertIn("dirids (Hover zeigt Ordnerpfad)", content)
        self.assertIn("4.00 KB", content)
        self.assertIn(f'title="0:{left}"', content)
        self.assertIn(f'title="0:{right}"', content)
        self.assertNotIn("photo.jpg", content)
        self.assertEqual(content.count('<tr class="file-group-row">'), 1)
        self.assertEqual(content.count(">0000001</span>"), 1)
        self.assertEqual(content.count(">0000002</span>"), 1)
        self.assertIn("3D-Baum: größte bestätigte Verzeichnisse", content)
        self.assertIn('id="tree-canvas"', content)
        self.assertIn("const pageSize = 26", content)
        self.assertIn('id="tree-sort" type="range" min="0" max="1"', content)
        self.assertIn(".tree-sort input{width:2.67rem}", content)
        self.assertIn("width:90%;margin-left:8%", content)
        self.assertIn("cell.id='first-active-group'", content)
        self.assertIn("id=\"files-table\"", content)
        self.assertLess(content.index('id="sort-option-a"'), content.index('id="tree-sort"'))
        self.assertLess(content.index('id="tree-sort"'), content.index('id="sort-option-b"'))
        self.assertIn('id="sort-option-a" class="sort-active"', content)
        self.assertIn('id="sort-option-b" class="sort-inactive"', content)
        self.assertIn("Verzeichnisgrösse</span>", content)
        self.assertIn("Anzahl Duplikate</span>", content)
        self.assertIn("sort-option-a').className=byGroupSize?", content)
        self.assertIn("payload.byFileSize", content)
        self.assertIn("payload.byGroupSize", content)
        self.assertIn("pages=makePages(allItems);", content)
        self.assertIn('aria-label="Erste">&lt;&lt;', content)
        self.assertIn('aria-label="Vorige">&lt;', content)
        self.assertIn('aria-label="Nächste">&gt;', content)
        self.assertIn('aria-label="Letzte">&gt;&gt;', content)
        self.assertIn('id="tree-copy">Copypaste', content)
        self.assertIn("copyBuffer.set(id", content)
        self.assertIn("navigator.clipboard.writeText(text)", content)
        self.assertIn("copiedIds.set(id,item.groupId||groupById.get(id)||0)", content)
        self.assertIn("new Set([...copiedIds.keys(),...copyBuffer.keys()])", content)
        self.assertIn("const text=['listof_files_job34'", content)
        self.assertIn("`${groupId}, ${ids.sort", content)
        self.assertIn("const isFinal = true", content)
        self.assertIn("copyButton.disabled=!isFinal", content)
        self.assertIn("localStorage.removeItem(storageKey)", content)
        self.assertIn("selected?'#e41a1c':copied?'#333333':color", content)
        self.assertIn("selected?3.5:copied?2.4:1.5", content)
        self.assertIn("isCopied?'#999999':item.color", content)
        self.assertIn("Seite ${currentPage+1} von ${pageCount} - Anzahl dargestellter Verzeichnisse", content)
        self.assertIn("sortControl.value='0'", content)
        self.assertIn("Gemeinsame Wurzel", content)
        self.assertIn("Ziehen zum Drehen, Mausrad zum Zoomen", content)
        self.assertIn("const rootRadius=14*p[2]", content)
        self.assertIn("if(hit.root)", content)
        self.assertIn("tip.style.fontSize='18px'", content)
        self.assertIn("Job 34: 100%", output.getvalue())

    def test_doubles_progress_html_renders_host_specific_tree_panels(self):
        tree = {"byFileSize": [], "byGroupSize": []}
        operation = app.client_operations(SimpleNamespace(mode="test"))
        page = operation._render_doubles_progress_html(
            50, "active", 10, 2, [], {0: tree, 1: tree}
        )
        self.assertEqual(page.count('<canvas '), 1)
        self.assertIn('id="tree-canvas"', page)
        self.assertNotIn('id="tree-canvas-host0"', page)
        self.assertIn('Hostid ${host}', page)

    def test_large_directory_groups_are_kept_together_and_ordered_first(self):
        rows = []
        for index in range(16):
            left_id = index * 2 + 1
            right_id = left_id + 1
            rows.append((
                left_id, f"/large/{left_id}", "large-hash", "0", right_id,
                f"/large/{right_id}", 1000 - index, 1000 - index, 1, 0, 1, 0,
            ))
        rows.append((
            100, "/small/a", "small-hash", "0", 101, "/small/b",
            10, 10, 1, 0, 1, 0,
        ))

        data = app.client_operations._largest_duplicate_directories(rows)
        by_file_size = data["byFileSize"]
        by_group_size = data["byGroupSize"]

        self.assertEqual(len(by_file_size), 34)
        self.assertEqual(len(by_group_size), 34)
        self.assertTrue(all(item["groupId"] == 1 for item in by_group_size[:32]))
        self.assertEqual({item["groupSize"] for item in by_group_size[:32]}, {32})
        self.assertEqual(
            {item["groupId"] for item in by_file_size},
            {item["groupId"] for item in by_group_size},
        )

    def test_doubles_progress_ctrl_c_lists_already_verified_duplicates(self):
        operation = app.client_operations(
            SimpleNamespace(mode="doubles-progress", jobid=34)
        )
        progress = [(34, "active", 4, 2), KeyboardInterrupt(), (34, "active", 4, 2)]
        with mock.patch.object(
            operation.database, "get_double_job_progress", side_effect=progress
        ) as get_progress, mock.patch.object(
            operation.database, "get_job",
            return_value={"jobid": 34, "hostid": 1, "last_activity": datetime(2026, 10, 28, 12, 34, 56)},
        ) as get_job, mock.patch.object(
            operation.database, "list_verified_double_pairs", return_value=[]
        ) as list_pairs, mock.patch.object(
            app.time, "sleep", side_effect=KeyboardInterrupt
        ), contextlib.redirect_stdout(io.StringIO()) as output:
            operation.show_doubles_progress(34)

        self.assertEqual(get_progress.call_count, 1)
        list_pairs.assert_called_once_with(34)
        self.assertIn("Job 34: 50%", output.getvalue())
        self.assertIn("Fortschrittsanzeige abgebrochen", output.getvalue())
        self.assertIn("there were some errors. incomplete check", output.getvalue())

        get_job.assert_called_once_with(34)
        self.assertIn("Job 34: jobs.last_activity = 2026-10-28 12:34:56 UTC", output.getvalue())
        self.assertIn("Tabelle directories 1 verändert", output.getvalue())
        self.assertNotIn("<hostid>", output.getvalue())

    def test_doubles_progress_caps_incomplete_closed_job_at_99_and_lists_verified_pairs(self):
        operation = app.client_operations(
            SimpleNamespace(mode="doubles-progress", jobid=34)
        )
        with tempfile.TemporaryDirectory() as temporary:
            left = os.path.join(temporary, "left")
            right = os.path.join(temporary, "right")
            os.makedirs(left)
            os.makedirs(right)
            for directory in (left, right):
                with open(os.path.join(directory, "copy.dat"), "wb") as stream:
                    stream.write(b"confirmed")
            pairs = [
                (1, left, "a" * 64, "0", 2, right),
                (2, right, "a" * 64, "0", 1, left),
            ]
            with mock.patch.object(
                operation.database, "get_double_job_progress",
                return_value=(34, "closed", 3, 2),
            ), mock.patch.object(
                operation.database, "get_job",
                return_value={"jobid": 34, "hostid": 1, "last_activity": datetime(2026, 10, 28, 12, 34, 56)},
            ) as get_job, mock.patch.object(
                operation.database, "list_verified_double_pairs", return_value=pairs
            ), contextlib.redirect_stdout(io.StringIO()) as output:
                operation.show_doubles_progress(34)
        self.assertIn("Job 34: 66% (2/3 geprüft)", output.getvalue())
        self.assertNotIn("100%", output.getvalue())
        self.assertIn("there were some errors. incomplete check", output.getvalue())
        self.assertIn("copy.dat", output.getvalue())


        get_job.assert_called_once_with(34)
        self.assertIn("Job 34: jobs.last_activity = 2026-10-28 12:34:56 UTC", output.getvalue())
        self.assertIn("Tabelle directories 1 verändert", output.getvalue())
        self.assertNotIn("<hostid>", output.getvalue())


class TreeHtmlTests(unittest.TestCase):
    def test_report_uses_existing_data_and_separate_filename(self):
        operation = client_operations(ClientArgumentParser().parse(['--doubles-tree-html', '34']))
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(operation.database, 'get_double_job_progress', return_value=(34, 'closed', 2, 2)), \
             patch.object(operation.database, 'get_double_job_batch', return_value=[(34, 0)]), \
             patch.object(operation.database, 'list_deep_double_tree_rows', return_value=[]) as pairs, \
             patch('doubles_output.Path.cwd', return_value=Path(directory)), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
            result = Path(directory) / 'doubles-tree-34.html'
            self.assertTrue(result.exists())
            self.assertIn('Binarycheck beendet', result.read_text())
            self.assertFalse((Path(directory) / 'doubles-progress-34.html').exists())
            pairs.assert_called_once_with(34)

    def test_tree_report_displays_partial_results_and_refreshes_at_completion(self):
        operation = client_operations(ClientArgumentParser().parse(['--doubles-tree-html', '34']))
        partial = [(7, '/partial', 'h', '0', 7, '/partial', 100, 100, 1, 0, 1, 0, 10)]
        final = [(9, '/final', 'h', '0', 9, '/final', 100, 100, 1, 0, 1, 0, 10)]
        progress = [(34, 'active', 2, 1), (34, 'active', 2, 1), (34, 'closed', 2, 2)]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(operation.database, 'get_double_job_progress', side_effect=progress), \
             patch.object(operation.database, 'get_double_job_batch', return_value=[(34, 10)]), \
             patch.object(operation.database, 'list_deep_double_tree_rows', side_effect=[partial, final]) as deep, \
             patch.object(operation.database, 'list_verified_double_pairs') as verified, \
             patch('doubles_output.Path.cwd', return_value=Path(directory)), \
             patch('doubles_output.time.monotonic', return_value=0), \
             patch('doubles_output.time.sleep') as sleep, \
             contextlib.redirect_stdout(io.StringIO()):
            path = Path(directory) / 'doubles-tree-34.html'
            def wait(_seconds):
                deep.assert_called_once_with(34)
                content = path.read_text()
                self.assertIn('data-dirid="7"', content)
                self.assertIn('Liste kann unvollständig sein', content)
            sleep.side_effect = wait
            operation.run()
            content = path.read_text()
            self.assertEqual(deep.call_args_list, [mock.call(34), mock.call(34)])
            sleep.assert_called_once_with(20)
            verified.assert_not_called()
            self.assertEqual(content.count('class="file-dirid"'), 1)
            self.assertIn('data-dirid="9"', content)
            self.assertNotIn('data-dirid="7"', content)
            self.assertIn('tmp_deepdoubles_result', content)
            self.assertIn('keine zusätzliche byteweise Bestätigung', content)
            self.assertIn('Hostid ${host}', content)

    def test_incomplete_closed_batch_displays_available_partial_results(self):
        operation = client_operations(ClientArgumentParser().parse(['--doubles-tree-html', '34']))
        rows = [(7, '/partial', 'h', '0', 7, '/partial', 100, 100, 1, 0, 1, 0, 0)]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(operation.database, 'get_double_job_progress', return_value=(34, 'closed', 2, 1)), \
             patch.object(operation.database, 'get_double_job_batch', return_value=[(34, 0)]), \
             patch.object(operation.database, 'list_deep_double_tree_rows', return_value=rows) as deep, \
             patch('doubles_output.Path.cwd', return_value=Path(directory)), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
            deep.assert_called_once_with(34)
            content = (Path(directory) / 'doubles-tree-34.html').read_text()
            self.assertIn('data-dirid="7"', content)
            self.assertIn('Liste kann unvollständig sein', content)
            self.assertIn('1/2 geprüft', content)

    def test_incomplete_batch_without_positive_results_displays_empty_partial_report(self):
        operation = client_operations(ClientArgumentParser().parse(['--doubles-tree-html', '34']))
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(operation.database, 'get_double_job_progress', return_value=(34, 'closed', 2, 1)), \
             patch.object(operation.database, 'get_double_job_batch', return_value=[(34, 0)]), \
             patch.object(operation.database, 'list_deep_double_tree_rows', return_value=[]) as deep, \
             patch('doubles_output.Path.cwd', return_value=Path(directory)), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
            deep.assert_called_once_with(34)
            content = (Path(directory) / 'doubles-tree-34.html').read_text()
            self.assertIn('Keine verschachtelten Kandidaten im bisher bestätigten Teilbestand', content)
            self.assertNotIn('Ergebnisse stehen erst', content)

    def test_tree_report_lists_all_members_of_same_hash_across_hosts(self):
        operation = client_operations(ClientArgumentParser().parse(['--doubles-tree-html', '34']))
        rows = [(dirid, '/'+str(dirid), 'same-hash', '0', dirid, '/'+str(dirid),
                 100, 100, 1, 0, 1, 0, hostid)
                for dirid, hostid in ((7, 0), (12, 0), (20, 10), (21, 10))]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(operation.database, 'get_double_job_progress', return_value=(34, 'closed', 2, 2)), \
             patch.object(operation.database, 'get_double_job_batch', return_value=[(34, 0)]), \
             patch.object(operation.database, 'list_deep_double_tree_rows', return_value=rows), \
             patch('doubles_output.Path.cwd', return_value=Path(directory)), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
            content = (Path(directory) / 'doubles-tree-34.html').read_text()
        table = content.split('<tbody>', 1)[1].split('</tbody>', 1)[0]
        self.assertEqual(table.count('class="file-group-row"'), 1)
        self.assertEqual(table.count('class="file-dirid"'), 4)
        for dirid in (7, 12, 20, 21):
            self.assertIn(f'data-dirid="{dirid}"', table)
        self.assertIn('Hostid ${host}', content)
        self.assertIn('Hostids</th>', content)


class BottommostTreeDuplicateTests(unittest.TestCase):
    @staticmethod
    def row(dirid, path, host=0, hash_='same'):
        return (dirid, path, hash_, '0', dirid, path, 100, 100, 1, 0, 1, 0, host)

    def test_deepest_paths_win_even_when_their_dirids_are_smaller(self):
        from doubles_output import doubles_output
        rows = [self.row(900, '/copy'), self.row(800, '/copy/branch'),
                self.row(1, '/copy/branch/deep'), self.row(2, '/copy/sibling')]
        result = doubles_output._bottommost_duplicate_tree_rows(rows)
        self.assertEqual([row[0] for row in result], [1, 2])
        self.assertEqual(len(rows), 4)

    def test_host_boundaries_case_and_complete_path_components_are_respected(self):
        from doubles_output import doubles_output
        rows = [self.row(1, '/a/b'), self.row(2, '/a/b2/leaf'),
                self.row(3, '/a/B/leaf'), self.row(4, '/a/b/leaf', host=1),
                self.row(5, '/a/b/./', host=2), self.row(6, '/a/b/leaf', host=2)]
        result = doubles_output._bottommost_duplicate_tree_rows(rows)
        self.assertEqual([row[0] for row in result], [1, 2, 3, 4, 6])

    def test_root_and_unsorted_sibling_branches_keep_only_their_deepest_candidates(self):
        from doubles_output import doubles_output
        rows = [self.row(1, '/a/b/c'), self.row(2, '/'), self.row(3, '/a'),
                self.row(4, '/a/d/e'), self.row(5, '/a/b')]
        self.assertEqual([row[0] for row in doubles_output._bottommost_duplicate_tree_rows(rows)], [1, 4])
        self.assertEqual(doubles_output._bottommost_duplicate_tree_rows([]), [])

    def test_generated_tree_table_and_canvas_data_use_bottommost_ids(self):
        operation = client_operations(ClientArgumentParser().parse(['--doubles-tree-html', '34']))
        rows = [self.row(900, '/left', hash_='parent'),
                self.row(901, '/right', hash_='parent'),
                self.row(7, '/left/leaf'), self.row(8, '/right/leaf')]
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(operation.database, 'get_double_job_progress', return_value=(34, 'closed', 2, 2)), \
             patch.object(operation.database, 'get_double_job_batch', return_value=[(34, 0)]), \
             patch.object(operation.database, 'list_deep_double_tree_rows', return_value=rows), \
             patch('doubles_output.Path.cwd', return_value=Path(directory)), \
             contextlib.redirect_stdout(io.StringIO()):
            operation.run()
            content = (Path(directory) / 'doubles-tree-34.html').read_text()
        for dirid in (7, 8):
            self.assertIn(f'data-dirid="{dirid}"', content)
            self.assertIn(f'"dirid": {dirid}', content)
        for dirid in (900, 901):
            self.assertNotIn(f'data-dirid="{dirid}"', content)
            self.assertNotIn(f'"dirid": {dirid}', content)


    def test_result_rows_with_special_or_unknown_entries_are_not_reclassified_by_renderer(self):
        from doubles_output import doubles_output
        row = list(self.row(7, '/copy/special'))
        row[8:12] = [0, 0, 0, 0]
        data = doubles_output._verified_pairs_html_data([tuple(row)])
        self.assertEqual([item[0] for _group, items in data for item in items], [7])


class ProgressHostPollingTests(unittest.TestCase):
    def test_host_filter_is_kept_on_each_poll(self):
        from doubles_output import doubles_output
        database = mock.Mock()
        database.get_double_job_progress.side_effect = [(34, 'active', 2, 1), (34, 'closed', 2, 2)]
        database.get_job.return_value = {'hostid': 1}
        operation = doubles_output(database)
        with mock.patch.object(operation, '_list_verified_duplicate_files'), \
             patch('doubles_output.time.sleep'), contextlib.redirect_stdout(io.StringIO()):
            operation.show_doubles_progress(hostid=1)
        self.assertEqual(database.get_double_job_progress.call_args_list,
                         [mock.call(None, hostid=1), mock.call(34, hostid=1)])

    def test_wrong_host_does_not_read_job_or_file_results(self):
        from doubles_output import doubles_output
        database = mock.Mock()
        database.get_double_job_progress.return_value = None
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            doubles_output(database).show_doubles_progress(34, hostid=2)
        database.get_job.assert_not_called()
        database.list_verified_double_pairs.assert_not_called()
        self.assertIn('Host-ID 2', output.getvalue())


class CrossHostReportTests(unittest.TestCase):
    def test_cross_host_result_is_not_read_from_remote_path_locally(self):
        from doubles_output import doubles_output
        output = doubles_output(mock.Mock())
        row = (20, '/same/path', 'hash', '0-1', 10, '/same/path', 5, 5, 1, 0, 1, 0, 1, 0)
        with mock.patch('doubles_output.os.walk') as walk, contextlib.redirect_stdout(io.StringIO()) as text:
            output._list_verified_duplicate_files(7, [row])
        walk.assert_not_called()
        self.assertIn('Host 1:20', text.getvalue())
        self.assertIn('Host 0:10', text.getvalue())

    def test_host_panels_do_not_assign_the_remote_directory_to_local_host(self):
        from doubles_output import doubles_output
        row = (20, '/local', 'hash', '0-1', 10, '/remote', 5, 5, 1, 0, 1, 0, 1, 0)
        local = doubles_output._largest_duplicate_directories([row], hostid_filter=1)
        remote = doubles_output._largest_duplicate_directories([row], hostid_filter=0)
        self.assertIn('/local', str(local))
        self.assertNotIn('/remote', str(local))
        self.assertIn('/remote', str(remote))
        self.assertNotIn('/local', str(remote))


class TreeSnapshotTests_doubles_output(unittest.TestCase):
    def test_active_batch_generates_static_multi_host_html_and_exits(self):
        db = mock.Mock()
        db.get_double_job_progress.return_value = (39,'active',10,1)
        db.get_double_job_batch.return_value = [(38,0),(39,1)]
        db.list_doubles_tree_snapshot_rows.return_value = [
            (2,'/Mac/a','child','0',2,'/Mac/a',10,10,1,0,1,0,1),
            (11,'/NAS/a','child','0',11,'/NAS/a',10,10,1,0,1,0,0),
        ]
        with tempfile.TemporaryDirectory() as folder:
            with mock.patch('doubles_output.Path.cwd', return_value=Path(folder)), \
                    mock.patch('doubles_output.time.sleep') as sleep, \
                    contextlib.redirect_stdout(io.StringIO()):
                path = doubles_output(db).show_doubles_tree(39)
            html = path.read_text()
            self.assertIn('/Mac/a', html)
            self.assertIn('/NAS/a', html)
            self.assertNotIn('http-equiv="refresh"', html)
            self.assertIn('Momentaufnahme', html)
            sleep.assert_not_called()
        db.list_deep_double_tree_rows.assert_not_called()
        db.create_binarycheck_job.assert_not_called()

    def test_one_panel_and_sortable_hostids_column_for_confirmed_cross_host_pair(self):
        rows = [(20,'/Mac/25HS','hash','1-0',10,'/NAS/25HS',100,100,1,1,1,1,1,0)]
        output = doubles_output(mock.Mock())
        data = output._verified_pairs_html_data(rows, group_across_hosts=True)
        tree = output._largest_duplicate_directories(rows)
        page = output._render_doubles_progress_html(39,'closed',1,1,data,tree,auto_refresh=False)
        self.assertEqual(page.count('<canvas '), 1)
        self.assertIn('Hostids</th>', page)
        self.assertIn('<td>0, 1</td>', page)
        self.assertIn('data-dirid="20"', page)
        self.assertIn('data-dirid="10"', page)
        self.assertIn('"hostid": 0', page)
        self.assertIn('"hostid": 1', page)
        self.assertEqual(page.count('aria-sort="none"'), 4)
        self.assertIn("header.addEventListener('click',sort)", page)
        Path('/tmp/shared-host-tree-preview.html').write_text(page)


class TestHostPrefixedHover(unittest.TestCase):
    def test_canvas_and_table_hover_begin_with_host_and_path(self):
        rows = [(20, '/Users/alex/_TEST/25HS', 'hash', '1-0', 10,
                 '/volume1/25HS', 100, 100, 1, 1, 1, 1, 1, 0)]
        output = doubles_output(mock.Mock())
        data = output._verified_pairs_html_data(rows, group_across_hosts=True)
        tree = output._largest_duplicate_directories(rows)
        page = output._render_doubles_progress_html(39, 'closed', 1, 1, data, tree, auto_refresh=False)
        self.assertIn('title="0:/volume1/25HS"', page)
        self.assertIn('title="1:/Users/alex/_TEST/25HS"', page)
        self.assertIn('${Number(hit.item.hostid)||0}:${hit.path}', page)


class TestAllSavedTreeResults(unittest.TestCase):
    def test_omitted_job_id_includes_all_jobs_not_only_latest_batch(self):
        database = mock.Mock()
        database.get_double_job_progress.side_effect = lambda id=None: (
            (39, 'closed', 1, 1) if id is None else (id, 'closed', 1, 1))
        database.get_double_job_batch.return_value = [(1,0), (38,0), (39,1)]
        database.list_doubles_tree_snapshot_rows.return_value = [
            (20,'/old-job', 'oldhash','0',10,'/copy',100,100,1,0,1,0,1,0)]
        with tempfile.TemporaryDirectory() as folder, \
                mock.patch('doubles_output.Path.cwd', return_value=Path(folder)), \
                mock.patch('doubles_output.time.sleep') as sleep, \
                contextlib.redirect_stdout(io.StringIO()):
            path = doubles_output(database).show_doubles_tree()
            self.assertEqual(path.name, 'doubles-tree-snapshot-all.html')
            self.assertIn('/old-job', path.read_text())
        database.get_double_job_batch.assert_called_once_with(None)
        database.list_doubles_tree_snapshot_rows.assert_called_once_with(None)
        self.assertEqual(database.get_double_job_progress.call_args_list,
                         [mock.call(None), mock.call(1), mock.call(38), mock.call(39)])
        sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
