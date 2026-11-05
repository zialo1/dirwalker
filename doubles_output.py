# version 0.25.2

"""Duplikatlisten, Pfadauflösung und HTML-Fortschrittsberichte für version22."""

__version__ = "0.25.2"

import html
import json
import logging
from logging.handlers import SysLogHandler
import multiprocessing
import os
import re
import signal
import socket
import sys
import time
import traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone


def _runtime():
    from version22 import ApplicationContext
    return ApplicationContext.runtime()

class doubles_output:
    """Prepare duplicate data and render console output and HTML reports."""

    def __init__(self, database):
        self.database = database

    @staticmethod
    def _human_readable_size(size_bytes):
        """Format a byte count with binary KB, MB, or GB units."""
        value = float(size_bytes)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if value < 1024 or unit == "TB":
                return f"{value:.2f} {unit}" if unit != "B" else f"{int(value)} B"
            value /= 1024


    def list_doubles(self):
        """List duplicate subtree roots and their matching paths."""
        print(
            "Suche in directories nach Verzeichnissen mit gleichem combined_sha256 "
            "und gleicher subtree_structure innerhalb desselben Hosts. "
            "Vollständig lesbare Unterbäume mit ausschließlich 0-Byte-Dateien und "
            "Verzeichnissen sowie Duplikate unterhalb bereits doppelter "
            "Verzeichnisse werden ausgelassen. Die Liste beruht auf gespeicherten "
            "Hashes, ohne erneuten Bytevergleich. "
            "Die Auswertung der Elternketten kann einige Zeit dauern.",
            flush=True,
        )
        rows = self.database.list_duplicate_subtree_roots()
        if not rows:
            print("Keine vollständig doppelten Unterbäume gefunden.")
            return

        print(
            "hostid | Gruppe | dirid | Pfad | subtree_structure | subtree_size_bytes"
        )
        previous_key = None
        group = 0
        for row in rows:
            dirid, path, structure, subtree_size_bytes, subtree_hash = row[:5]
            hostid = row[5] if len(row) > 5 else 0
            key = (hostid, subtree_hash)
            if key != previous_key:
                group += 1
                previous_key = key
            print(
                f"{hostid} | {group} | {dirid:07d} | {path} | {structure} "
                f"| {subtree_size_bytes}"
            )


    def transform_double_dirnames(self):
        """Read dirids from stdin and write their directory names to a file."""
        print(
            "geben sie ',' gefolgt von einer liste von dirids getrennt nur durch "
            "spaces ein. Beenden sie die Eingabe mit '.' auf einer neuen Zeile"
        )
        dirids = []
        seen = set()
        output_filename = "listof_files.txt"
        first_input_line = True
        while True:
            line = sys.stdin.readline()
            if line == "":
                break
            line = line.strip()
            if line == ".":
                break
            if first_input_line:
                first_input_line = False
                for prefix in ("listof_files_job", "selected_dirsids_"):
                    if line.startswith(prefix) and line[len(prefix):].isdecimal():
                        output_filename = f"listof_files_job{line[len(prefix):]}.txt"
                        break
                else:
                    prefix = None
                if prefix is not None:
                    continue
            if "," in line:
                line = line.split(",", 1)[1]
            for value in line.split():
                if value.isdecimal():
                    dirid = int(value)
                    if dirid not in seen:
                        seen.add(dirid)
                        dirids.append(dirid)

        if not dirids:
            print("Keine dirids eingegeben.")
            return

        paths_by_id = {
            int(dirid): path for dirid, path in self.database.get_directory_paths(dirids)
        }
        output_lines = []
        for dirid in dirids:
            path = paths_by_id.get(dirid)
            if path is None:
                output_lines.append(f"{dirid:07d} | nicht gefunden")
            else:
                output_lines.append(str(path))

        output_path = Path.cwd() / output_filename
        output_path.write_text("\n".join(output_lines) + "\n", encoding="utf-8")
        print(f"{len(output_lines)} Verzeichnisnamen gespeichert: {output_path}")


    def list_double_jobs(self):
        """Show the persisted state of bytewise comparisons."""
        rows = self.database.list_double_jobs()
        if rows is None:
            print("Tabelle tmp_doubles_letchecked ist noch nicht eingerichtet.")
            return
        if not rows:
            print("Keine Binarycheck-Einträge vorhanden.")
            return
        print("jobid | hostid | hostidB | left_dirid | right_dirid | status | method | source_pair | finished_at | error | runid | requested")
        for row in rows:
            jobid, hostid, left, right, status, method, source_left, source_right, finished, error, runid, requested = row[:12]
            hostid_b = row[12] if len(row) > 12 else hostid
            source = f"{source_left}:{source_right}" if source_left is not None else "-"
            print(f"{jobid} | {hostid} | {hostid_b} | {left:07d} | {right:07d} | {status} | "
                  f"{method or '-'} | {source} | {finished or '-'} | {error or '-'} | "
                  f"{runid} | {bool(requested)}")


    def show_doubles_progress(self, jobid=None, hostid=None):
        """Poll a binarycheck job every 20 seconds and show verified file copies."""
        selected_hostid = hostid
        def read_progress(current_jobid):
            if selected_hostid is None:
                return self.database.get_double_job_progress(current_jobid)
            return self.database.get_double_job_progress(current_jobid, hostid=selected_hostid)
        progress = read_progress(jobid)
        if progress is None:
            if selected_hostid is None:
                print("need to start proess with --doubles-letcheck")
            else:
                print(f"Kein passender Binarycheck-Job für Host-ID {selected_hostid} gefunden.")
            return

        jobid, status, total, checked = progress
        job = self.database.get_job(jobid)
        hostid = job.get("hostid") if job else None
        host_text = str(hostid) if hostid is not None else "unbekannt"
        print(
            "Es werden Veränderungen in der Tabelle tmp_doubles_letchecked beobachtet. "
            f"Diese wird von einem Server zu Kandidaten aus Tabelle directories {host_text} verändert. "
            "Der Server prüft auf identische Verzeichnisse, wenn diese Tabelle angibt, "
            "dass sie es vermutlich sind.",
            flush=True,
        )
        last_activity = job.get("last_activity") if job else None
        activity_text = (
            f"{last_activity:%Y-%m-%d %H:%M:%S} UTC"
            if last_activity is not None else "noch nicht vorhanden"
        )
        print(f"Job {jobid}: jobs.last_activity = {activity_text}", flush=True)
        verified_pairs_cache = self.database.list_verified_double_pairs(jobid)
        cache_updated_at = time.monotonic()
        while True:
            if status == "closed":
                incomplete = checked < total
                percentage = 100 if not incomplete else min(
                    99, int(checked * 100 / total) if total else 0
                )
                sys.stdout.write(
                    f"\r\x1b[2KJob {jobid}: {percentage}% "
                    f"({checked}/{total} geprüft)\n"
                )
                sys.stdout.flush()
                if incomplete:
                    print("there were some errors. incomplete check")
                self._list_verified_duplicate_files(jobid)
                return
            if status in ("error", "stopped", "paused"):
                percentage = min(99, int(checked * 100 / total) if total else 0)
                sys.stdout.write(
                    f"\r\x1b[2KJob {jobid}: {percentage}% ({checked}/{total} geprüft; {status})\n"
                )
                sys.stdout.flush()
                print("there were some errors. incomplete check")
                self._list_verified_duplicate_files(jobid)
                return

            percentage = int(checked * 100 / total) if total else 0
            if status == "active":
                line = f"Job {jobid}: {percentage}% ({checked}/{total} geprüft)"
            else:
                line = f"Job {jobid}: wartet auf Server ({status})"
            sys.stdout.write(f"\r\x1b[2K{line}")
            sys.stdout.flush()
            try:
                time.sleep(20)
                progress = read_progress(jobid)
                if (
                    progress is not None
                    and time.monotonic() - cache_updated_at >= 90
                ):
                    verified_pairs_cache = self.database.list_verified_double_pairs(jobid)
                    cache_updated_at = time.monotonic()
            except KeyboardInterrupt:
                # Use only cached values after Ctrl-C: do not query the DB here.
                percentage = min(99, int(checked * 100 / total) if total else 0)
                sys.stdout.write(
                    f"\r\x1b[2KJob {jobid}: {percentage}% "
                    f"({checked}/{total} geprüft; Fortschrittsanzeige abgebrochen)\n"
                )
                sys.stdout.flush()
                print("there were some errors. incomplete check")
                print("Ausgabe aus Cache; Stand kann veraltet sein.")
                self._list_verified_duplicate_files(
                    jobid, rows=verified_pairs_cache
                )
                return
            if progress is None:
                sys.stdout.write(f"\r\x1b[2KJob {jobid} wurde nicht gefunden.\n")
                sys.stdout.flush()
                return
            jobid, status, total, checked = progress


    def _list_verified_duplicate_files(self, jobid, rows=None):
        """List files present in multiple bytewise-verified directory copies."""
        if rows is None:
            rows = self.database.list_verified_double_pairs(jobid)
        groups = {}
        remote_pairs = False
        for row in rows:
            dirid, left_path, subtree_hash, structure, other_dirid, right_path = row[:6]
            if len(row) > 13 and row[12] != row[13]:
                remote_pairs = True
                print(f"Bytegleich: Host {row[12]}:{dirid} {left_path} <-> Host {row[13]}:{other_dirid} {right_path}")
                continue
            key = (subtree_hash, structure)
            group = groups.setdefault(key, {})
            group[dirid] = left_path
            group[other_dirid] = right_path

        if not groups:
            if remote_pairs:
                return
            print("Keine byteweise bestätigten doppelten Verzeichnisse gefunden.")
            return

        print("Gruppe | Ordnerpfad | Datei | Größe | Kopien")
        listed = False
        for group_id, key in enumerate(sorted(groups), start=1):
            copies_by_relative_path = {}
            missing_dirids = []
            for root_path in sorted(set(groups[key].values())):
                if not os.path.isdir(root_path):
                    missing_dirids.extend(
                        dirid for dirid, path in groups[key].items()
                        if path == root_path
                    )
                    continue

                def raise_walk_error(error):
                    raise error

                for folder, dirnames, filenames in os.walk(
                    root_path, onerror=raise_walk_error, followlinks=False
                ):
                    dirnames.sort()
                    for filename in sorted(filenames):
                        file_path = os.path.join(folder, filename)
                        if os.path.islink(file_path) or not os.path.isfile(file_path):
                            continue
                        relative_path = os.path.relpath(file_path, root_path)
                        copies_by_relative_path.setdefault(relative_path, []).append(
                            (folder, filename, os.path.getsize(file_path))
                        )

            if missing_dirids:
                listed = True
                formatted_dirids = ", ".join(
                    f"{dirid:07d}" for dirid in sorted(set(missing_dirids))
                )
                print(f"{group_id} | {formatted_dirids}")

            for relative_path in sorted(copies_by_relative_path):
                copies = copies_by_relative_path[relative_path]
                if len(copies) < 2:
                    continue
                listed = True
                for folder, filename, size in copies:
                    print(
                        f"{group_id} | {folder} | {filename} | "
                        f"{self._human_readable_size(size)} | {len(copies)}"
                    )
        if not listed:
            print("Keine Dateien mit mehreren Kopien gefunden.")


    @staticmethod
    def _bottommost_duplicate_tree_rows(rows):
        """Keep deepest candidate paths per host, independently of numeric dirids.

        Walk stored path components only: no filesystem access, SQL self-join,
        or quadratic comparison of every candidate against every other one.
        """
        paths = {(int(row[12] or 0) if len(row) > 12 else 0,
                  os.path.normpath(row[1])) for row in rows}
        ancestors = set()
        for hostid, path in paths:
            parent = os.path.dirname(path)
            while parent != path:
                key = (hostid, parent)
                if key in ancestors:
                    # This ancestor's own ancestors were already visited.
                    break
                if key in paths:
                    ancestors.add(key)
                path, parent = parent, os.path.dirname(parent)
        return [row for row in rows
                if (int(row[12] or 0) if len(row) > 12 else 0,
                    os.path.normpath(row[1])) not in ancestors]


    @staticmethod
    def _verified_pairs_html_data(rows, *, group_across_hosts=False):
        """Build groups from cached rows without inspecting the local filesystem."""
        groups = {}
        for row in rows:
            (
                dirid, left_path, subtree_hash, structure, other_dirid, right_path,
                left_size, right_size,
            ) = row[:8]
            hostid = row[12] if len(row) > 12 else 0
            key = (0, subtree_hash, "") if group_across_hosts else (hostid, subtree_hash, structure)
            group = groups.setdefault(key, {})
            group[dirid] = (left_path, left_size)
            group[other_dirid] = (right_path, right_size)

        return [
            (
                group_id,
                [
                    (dirid, path, size)
                    for dirid, (path, size) in sorted(groups[key].items())
                ],
            )
            for group_id, key in enumerate(sorted(groups), start=1)
        ]


    @staticmethod
    def _largest_duplicate_directories(rows, *, hash_group_ids=None, hostid_filter=None):
        """Return duplicate-directory groups, putting groups larger than 26 first."""
        groups = {}
        for row in rows:
            left_id, left_path, _hash, _structure, right_id, right_path, left_size, right_size = row[:8]
            hostid = row[12] if len(row) > 12 else 0
            right_host = row[13] if len(row) > 13 else hostid
            for side_host, dirid, path, size in ((hostid, left_id, left_path, left_size),
                                                (right_host, right_id, right_path, right_size)):
                if hostid_filter is not None and side_host != hostid_filter:
                    continue
                group = groups.setdefault((side_host, _hash, _structure), {})
                group[dirid] = {"dirid": dirid, "path": path, "size": int(size or 0), "hostid": side_host}

        stable_group_ids = {
            key: hash_group_ids[key[1]] if hash_group_ids is not None else index
            for index, key in enumerate(sorted(groups), start=1)
        }

        def ordered_items(sort_by_group_size):
            keys = sorted(
                groups,
                key=lambda key: (
                    0 if len(groups[key]) > 26 else 1,
                    -len(groups[key]) if sort_by_group_size else
                    -max((item["size"] for item in groups[key].values()), default=0),
                    -max((item["size"] for item in groups[key].values()), default=0)
                    if sort_by_group_size else -len(groups[key]),
                    stable_group_ids[key],
                ),
            )
            result = []
            for key in keys:
                members = sorted(
                    groups[key].values(),
                    key=lambda item: (-item["size"], item["dirid"]),
                )
                for item in members:
                    result.append(dict(
                        item,
                        groupId=stable_group_ids[key],
                        groupSize=len(members),
                    ))
            return result

        return {
            "byFileSize": ordered_items(sort_by_group_size=False),
            "byGroupSize": ordered_items(sort_by_group_size=True),
        }


    @staticmethod
    def _render_3d_tree(tree_data, jobid, final=False, host_label=None, id_suffix=""):
        """Render a dependency-free, rotatable 3D tree for modern browsers."""
        safe_data = json.dumps(tree_data, ensure_ascii=False).replace("<", "\\u003c")
        label = f" ({html.escape(host_label)})" if host_label else ""
        markup = r"""<section class="tree-section">
<h2>3D-Baum: größte bestätigte Verzeichnisse__HOST_LABEL__</h2>
<p>Gemeinsame Wurzel__HOST_LABEL__: <code id="tree-root"></code>. Ziehen zum Drehen, Mausrad zum Zoomen.</p>
<style>.tree-wrap{position:relative;width:90%;margin-left:8%;height:620px;border:1px solid #bbb;background:#fbfcfe;touch-action:none}
#tree-canvas{width:100%;height:100%;display:block;cursor:grab}
.tree-navigation{display:flex;align-items:center;width:90%;margin:.75rem 0 .5rem 8%;gap:.5rem}
.tree-nav-buttons{display:flex;gap:.5rem;margin-left:auto}.tree-navigation button{padding:.35rem .7rem}
.tree-sort-row{display:flex;justify-content:flex-end;width:90%;margin:.75rem 0 0 8%}
.tree-copy-controls{margin:1rem 0 0 8%}.tree-copy-controls button{padding:.4rem .8rem}
.tree-sort{display:flex;align-items:center;gap:.4rem}.tree-sort input{width:2.67rem}
.sort-active{text-decoration:underline;color:#17212b;font-weight:700}.sort-inactive{color:#999}
.tree-tip{display:none;position:absolute;z-index:2;white-space:pre-line;background:#17212b;color:white;padding:.55rem .7rem;border-radius:5px;pointer-events:none;max-width:250px;box-shadow:0 2px 8px #0005}</style>
<div class="tree-navigation"><span id="tree-page"></span><div class="tree-nav-buttons">
<button id="tree-first" title="Erste" aria-label="Erste">&lt;&lt;</button>
<button id="tree-prev" title="Vorige" aria-label="Vorige">&lt;</button>
<button id="tree-next" title="Nächste" aria-label="Nächste">&gt;</button>
<button id="tree-last" title="Letzte" aria-label="Letzte">&gt;&gt;</button></div></div>
<div class="tree-wrap"><canvas id="tree-canvas" aria-label="Drehbarer 3D-Verzeichnisbaum"></canvas>
<div id="tree-tip" class="tree-tip"></div></div>
<div class="tree-sort-row">
<div class="tree-sort"><span>Sortierung</span>
<span id="sort-option-a" class="sort-active" title="Gesamtgröße des Verzeichnis-Unterbaums">Verzeichnisgrösse</span>
<input id="tree-sort" type="range" min="0" max="1" step="1" value="0" aria-label="Sortierung: Verzeichnisgrösse oder Anzahl Duplikate">
<span aria-hidden="true">↔</span>
<span id="sort-option-b" class="sort-inactive" title="Anzahl der Verzeichnisse in einer Duplikatgruppe">Anzahl Duplikate</span></div></div>
<div class="tree-copy-controls"><button id="tree-copy">Copypaste</button> <span>Hinweis:</span>
<span id="tree-copy-state"></span></div>
<script type="application/json" id="tree-data">__TREE_DATA__</script>
<script>
(() => {
  const payload = JSON.parse(document.getElementById('tree-data').textContent);
  const canvas = document.getElementById('tree-canvas');
  const ctx = canvas.getContext('2d');
  const tip = document.getElementById('tree-tip');
  const pageLabel = document.getElementById('tree-page');
  const rootLabel = document.getElementById('tree-root');
  const sortControl = document.getElementById('tree-sort');
  sortControl.value='0';
  let allItems = payload.byFileSize || [];
  const isFinal = __FINAL__;
  const storageKey = 'filewalker-copypaste-__JOBID__';
  const groupById=new Map(allItems.map(item=>[String(item.dirid),item.groupId||0]));
  const copyBuffer = new Map(), copiedIds = new Map();
  try{localStorage.removeItem(storageKey);}catch(_e){}
  const copyState=document.getElementById('tree-copy-state');
  const copyButton=document.getElementById('tree-copy');
  function saveCopyState(){
    const copied=Array.from(copiedIds,([dirid,groupId])=>({dirid,groupId}));
    try{localStorage.setItem(storageKey,JSON.stringify({pending:Array.from(copyBuffer.values()),copied}));}catch(_e){}
    copyState.textContent=`Zwischenspeicher: ${copyBuffer.size} ausgewählt, ${copiedIds.size} kopiert`;
    refreshFileListMarks();
  }
  document.addEventListener('file-table-sorted',()=>refreshFileListMarks());
  function refreshFileListMarks(){
    const selected=new Set([...copyBuffer.keys(),...copiedIds.keys()]);
    document.querySelectorAll('.file-dirid').forEach(span=>{
      span.classList.toggle('selected-dirid',selected.has(span.dataset.dirid));
    });
    let firstActiveGroup=true;
    document.querySelectorAll('.file-group-row').forEach(row=>{
      const hasSelected=Array.from(row.querySelectorAll('.file-dirid'))
        .some(span=>selected.has(span.dataset.dirid));
      const cell=row.querySelector('.file-group-id');
      const groupId=cell.dataset.groupId;
      row.id=`file-group-row-${groupId}`;
      cell.removeAttribute('id');
      if(hasSelected){
        cell.innerHTML=`<a href="#file-group-row-${groupId}">${groupId}*</a>`;
        if(firstActiveGroup){cell.id='first-active-group';firstActiveGroup=false;}
      }else{
        cell.textContent=groupId;
      }
    });
  }
  copyButton.disabled=!isFinal;
  const pageSize = 26;
  function makePages(items){
    const grouped=new Map();
    items.forEach(item=>{const id=item.groupId||0;if(!grouped.has(id))grouped.set(id,[]);grouped.get(id).push(item);});
    const groups=Array.from(grouped.values());
    const large=groups.filter(group=>group.length>pageSize);
    const regular=groups.filter(group=>group.length<=pageSize);
    const pages=large.map(group=>group.slice());
    const used=new Set();
    for(let i=0;i<regular.length;i++){
      if(used.has(i))continue;
      let page=[],groupCount=0,next=i;
      while(next<regular.length&&groupCount<7){
        if(used.has(next)){next++;continue;}
        const group=regular[next];
        if(page.length+group.length>pageSize)break;
        page.push(...group);groupCount++;used.add(next);next++;
      }
      // Allow one eighth group only when seven groups still show fewer than 16 items.
      if(groupCount===7&&page.length<16&&next<regular.length&&!used.has(next)){
        const extra=regular[next];
        if(page.length+extra.length<=pageSize){
          page.push(...extra);used.add(next);
        }
      }
      pages.push(page);
    }
    return pages;
  }
  let pages=makePages(allItems);
  const formPalette = [
    'circle','triangle','square','diamond','star','pentagon','cross','plus',
    'heart','crescent','chevron','kite','oval'
  ];
  const groupColors = [
    '#1f77b4','#ff7f0e','#2ca02c','#d62728','#9467bd','#8c564b','#e377c2',
    '#7f7f7f','#bcbd22','#17becf','#393b79','#637939','#8c6d31','#843c39'
  ];
  let nodes = [], edges = [], points = [], center = [0,0,0], worldSpan = 1;
  let yaw = -0.35, pitch = 0.18, zoom = 1, drag = null, dragged = false, currentPage = 0;
  function makeTree(items) {
    const commonPath='Hosts';
    const root={name:'Hosts',path:'',children:[],items:[],_children:{}};
    const hostPrefixes=new Map();
    for(const host of new Set(items.map(item=>Number(item.hostid)||0))){
      const paths=items.filter(item=>(Number(item.hostid)||0)===host)
        .map(item=>(item.path||'/').replace(/\\/g,'/').split('/').filter(Boolean));
      let length=paths.length?Math.min(...paths.map(path=>path.length)):0;
      for(let i=0;i<length;i++){if(!paths.every(path=>path[i]===paths[0][i])){length=i;break;}}
      hostPrefixes.set(host,Math.max(0,length-1));
    }
    const logSizes=items.map(item=>Math.log1p(Math.max(0,Number(item.size)||0)));
    const minLogSize=logSizes.length?Math.min(...logSizes):0;
    const maxLogSize=logSizes.length?Math.max(...logSizes):0;
    const pageItems=items.map(item=>{
      const logSize=Math.log1p(Math.max(0,Number(item.size)||0));
      const sizeRatio=maxLogSize===minLogSize ? 0.5 : (logSize-minLogSize)/(maxLogSize-minLogSize);
      const groupIndex=Math.max(0,Math.floor(Number(item.groupId)||0));
      const colorIndex=groupIndex%groupColors.length;
      const formIndex=groupIndex%formPalette.length;
      return Object.assign({},item,{
        sizeRatio,color:groupColors[colorIndex],shape:formPalette[formIndex],
        sizeLabel:formatSize(item.size)
      });
    });
    pageItems.forEach(item=>{
      const host=Number(item.hostid)||0, hostKey=String(host);
      const allParts=(item.path||'/').replace(/\\/g,'/').split('/').filter(Boolean);
      const commonLength=hostPrefixes.get(host)||0;
      const parts=allParts.slice(commonLength);
      let path='/'+allParts.slice(0,commonLength).join('/');
      if(!root._children[hostKey]){
        const branch={name:`Hostid ${host}`,hostid:host,path,children:[],items:[],_children:{}};
        root._children[hostKey]=branch;root.children.push(branch);
      }
      let node=root._children[hostKey];
      parts.forEach(part=>{
        path=(path==='/'?'':path)+'/'+part;
        if(!node._children[part]){
          const child={name:part,path,children:[],items:[],_children:{}};
          node._children[part]=child;node.children.push(child);
        }
        node=node._children[part];
      });
      node.items.push(item);
    });
    function selectedItems(node){return [...(node.items||[]),...(node.children||[]).flatMap(selectedItems)];}
    function pathDistance(a,b){
      const pa=(a||'').split('/').filter(Boolean),pb=(b||'').split('/').filter(Boolean);
      let common=0;while(common<pa.length&&common<pb.length&&pa[common]===pb[common])common++;
      return pa.length+pb.length-2*common;
    }
    function branchDistance(a,b){
      const left=selectedItems(a),right=selectedItems(b);
      if(!left.length||!right.length)return Number.MAX_VALUE;
      let total=0;left.forEach(x=>right.forEach(y=>{total+=pathDistance(x.path,y.path);}));
      return total/(left.length*right.length);
    }
    function nearestNeighborOrder(children){
      const remaining=children.slice(),ordered=[];
      if(!remaining.length)return ordered;
      const meanDepth=node=>{const items=selectedItems(node);return items.length?items.reduce((n,item)=>n+(item.path||'').split('/').filter(Boolean).length,0)/items.length:Infinity;};
      remaining.sort((a,b)=>meanDepth(a)-meanDepth(b)||a.name.localeCompare(b.name));
      ordered.push(remaining.shift());
      while(remaining.length){
        const previous=ordered[ordered.length-1];
        remaining.sort((a,b)=>branchDistance(previous,a)-branchDistance(previous,b)||a.name.localeCompare(b.name));
        ordered.push(remaining.shift());
      }
      return ordered;
    }
    function clean(node){node.children=nearestNeighborOrder(node.children);delete node._children;node.children.forEach(clean);}
    clean(root);
    return {root,commonPath};
  }
  function formatSize(bytes){let n=Number(bytes)||0;for(const unit of ['B','KB','MB','GB','TB']){if(n<1024||unit==='TB')return unit==='B'?`${Math.round(n)} B`:`${(n).toFixed(2)} ${unit}`;n/=1024;}return `${n.toFixed(2)} TB`;}
  function normalize(v) { const n=Math.hypot(v[0],v[1],v[2])||1; return v.map(x=>x/n); }
  function cross(a,b) { return [a[1]*b[2]-a[2]*b[1],a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]]; }
  function rootDirection(index,count) {
    let azimuth, tilt;
    if (count > 3) {
      // Main axes step around the trunk by exactly one seventh of a turn.
      azimuth = index * 2 * Math.PI / 7;
      tilt = 50 * Math.PI / 180;
    } else {
      // Up to three main axes start 60 degrees backwards from vertical.
      azimuth = Math.PI + index * 2 * Math.PI / Math.max(1,count);
      tilt = 60 * Math.PI / 180;
    }
    return [Math.sin(tilt)*Math.cos(azimuth),Math.cos(tilt),Math.sin(tilt)*Math.sin(azimuth)];
  }
  function coneDirection(axis,index,count) {
    if (count <= 1) return axis;
    const reference = Math.abs(axis[1]) < .88 ? [0,1,0] : [0,0,1];
    const u = normalize(cross(axis,reference)), v = normalize(cross(axis,u));
    const azimuth = index * 2 * Math.PI / count, cone = 16 * Math.PI / 180;
    return normalize([
      axis[0]*Math.cos(cone)+Math.sin(cone)*(u[0]*Math.cos(azimuth)+v[0]*Math.sin(azimuth)),
      axis[1]*Math.cos(cone)+Math.sin(cone)*(u[1]*Math.cos(azimuth)+v[1]*Math.sin(azimuth)),
      axis[2]*Math.cos(cone)+Math.sin(cone)*(u[2]*Math.cos(azimuth)+v[2]*Math.sin(azimuth))
    ]);
  }
  function layout(node, depth, position, axis) {
    node.depth = depth; [node.x,node.y,node.z] = position;
    nodes.push(node);
    const children = node.children || [];
    children.forEach((child, index) => {
      const direction = depth === 0
        ? rootDirection(index, children.length)
        : coneDirection(axis, index, children.length);
      const next = [
        position[0] + direction[0],
        position[1] + direction[1],
        position[2] + direction[2]
      ];
      layout(child, depth + 1, next, direction);
      edges.push([node, child]);
    });
  }
  function layoutPage(items) {
    nodes=[];edges=[];points=[];
    const built=makeTree(items), root=built.root;
    rootLabel.textContent=built.commonPath;
    if(!items.length){worldSpan=1;center=[0,0,0];draw();return;}
    layout(root, 0, [0,0,0], [0,1,0]);
    const bounds=['x','y','z'].map(axis=>[
      Math.min(...nodes.map(n=>n[axis])),Math.max(...nodes.map(n=>n[axis]))
    ]);
    center=bounds.map(([a,b])=>(a+b)/2);
    worldSpan=Math.max(1,...bounds.map(([a,b])=>b-a));
    draw();
  }
  function setPage(page){
    const pageCount=Math.max(1,pages.length);
    currentPage=Math.max(0,Math.min(pageCount-1,page));
    const shown=pages[currentPage]||[];
    const largestSize=shown.reduce((largest,item)=>Math.max(largest,Number(item.size)||0),0);
    const largestLabel=largestSize<0.3*1024*1024
      ? `${(largestSize/1024).toFixed(2)} KB`
      : `${(largestSize/(1024*1024)).toFixed(2)} MB`;
    pageLabel.textContent=`Seite ${currentPage+1} von ${pageCount} - Anzahl dargestellter Verzeichnisse: ${shown.length} - grösstes Verzeichnis ${largestLabel}`;
    document.getElementById('tree-first').disabled=currentPage===0;
    document.getElementById('tree-prev').disabled=currentPage===0;
    document.getElementById('tree-next').disabled=currentPage>=pageCount-1;
    document.getElementById('tree-last').disabled=currentPage>=pageCount-1;
    try{location.hash=`page=${currentPage}`;}catch(_e){}
    layoutPage(shown);
  }
  sortControl.addEventListener('input',e=>{
    const byGroupSize=e.target.value==='1';
    allItems=byGroupSize?(payload.byGroupSize||[]):(payload.byFileSize||[]);
    pages=makePages(allItems);
    document.getElementById('sort-option-a').className=byGroupSize?'sort-inactive':'sort-active';
    document.getElementById('sort-option-b').className=byGroupSize?'sort-active':'sort-inactive';
    setPage(0);
  });
  window.addEventListener('pageshow',()=>{
    if(sortControl.value==='0')return;
    sortControl.value='0';
    allItems=payload.byFileSize||[];
    pages=makePages(allItems);
    document.getElementById('sort-option-a').className='sort-active';
    document.getElementById('sort-option-b').className='sort-inactive';
    setPage(0);
  });
  function resize() {
    const rect = canvas.getBoundingClientRect();
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.max(1, rect.width * ratio);
    canvas.height = Math.max(1, rect.height * ratio);
    ctx.setTransform(ratio,0,0,ratio,0,0);
    draw();
  }
  function project(node) {
    const rect = canvas.getBoundingClientRect();
    const unit = Math.min(rect.width,rect.height)*.72/worldSpan;
    const x = (node.x-center[0])*unit, y = (node.y-center[1])*unit, z = (node.z-center[2])*unit;
    const cy = Math.cos(yaw), sy = Math.sin(yaw);
    const x1 = x * cy - z * sy, z1 = x * sy + z * cy;
    const cp = Math.cos(pitch), sp = Math.sin(pitch);
    const y1 = y * cp - z1 * sp, z2 = y * sp + z1 * cp;
    const perspective = 850 / (850 + z2);
    return [rect.width / 2 + x1 * perspective * zoom,
            rect.height / 2 + y1 * perspective * zoom, perspective];
  }
  function drawShape(x,y,r,shape,color,selected,copied) {
    ctx.save(); ctx.translate(x,y); ctx.fillStyle=color;
    ctx.strokeStyle=selected?'#e41a1c':copied?'#333333':color;
    ctx.lineWidth=selected?3.5:copied?2.4:1.5;
    ctx.beginPath();
    const sides={triangle:3,square:4,diamond:4,pentagon:5};
    if (sides[shape]) {
      const rotation=shape==='square'?Math.PI/4:0;
      for(let i=0;i<sides[shape];i++){
        const angle=-Math.PI/2+rotation+i*2*Math.PI/sides[shape];
        const px=Math.cos(angle)*r,py=Math.sin(angle)*r;
        if(i===0)ctx.moveTo(px,py);else ctx.lineTo(px,py);
      }
    } else if (shape === 'star') {
      for(let i=0;i<10;i++){const a=-Math.PI/2+i*Math.PI/5, rr=i%2?r*.45:r; const px=Math.cos(a)*rr,py=Math.sin(a)*rr; if(i===0)ctx.moveTo(px,py);else ctx.lineTo(px,py);}
    } else if (shape === 'cross' || shape === 'plus') {
      const arm=shape==='cross'?r*.32:r*.24, outer=r, inner=r*.48;
      const points=shape==='cross'
        ? [[-arm,-outer],[arm,-inner],[outer,-arm],[inner,arm],[arm,outer],[-arm,inner],[-outer,arm],[-inner,-arm]]
        : [[-arm,-outer],[arm,-outer],[arm,-arm],[outer,-arm],[outer,arm],[arm,arm],[arm,outer],[-arm,outer],[-arm,arm],[-outer,arm],[-outer,-arm],[-arm,-arm]];
      points.forEach(([px,py],i)=>{if(i===0)ctx.moveTo(px,py);else ctx.lineTo(px,py);});
    } else if (shape === 'heart') {
      ctx.moveTo(0,r*.88);ctx.bezierCurveTo(-r*.2,r*.62,-r,r*.05,-r,-r*.38);
      ctx.bezierCurveTo(-r,-r*.95,-r*.25,-r,r*.0,-r*.48);
      ctx.bezierCurveTo(r*.25,-r,r,-r*.95,r,-r*.38);
      ctx.bezierCurveTo(r,r*.05,r*.2,r*.62,0,r*.88);
    } else if (shape === 'crescent') {
      ctx.arc(-r*.12,0,r,-Math.PI/2,Math.PI/2,false);
      ctx.quadraticCurveTo(r*.92,r*.05,r*.46,-r*.72);
      ctx.quadraticCurveTo(r*.18,-r*.98,-r*.12,-r,false);
    } else if (shape === 'chevron') {
      [[-r,-r*.7],[0,-r*.7],[r,r*.05],[0,r*.8],[-r,r*.8],[-r*.25,r*.05]]
        .forEach(([px,py],i)=>{if(i===0)ctx.moveTo(px,py);else ctx.lineTo(px,py);});
    } else if (shape === 'kite') {
      [[0,-r],[r*.72,-r*.12],[0,r],[-r*.72,-r*.12]]
        .forEach(([px,py],i)=>{if(i===0)ctx.moveTo(px,py);else ctx.lineTo(px,py);});
    } else if (shape === 'oval') {
      ctx.ellipse(0,0,r*.72,r,0,0,Math.PI*2);
    } else { ctx.arc(0,0,r,0,Math.PI*2); }
    ctx.closePath();ctx.fill();ctx.stroke();ctx.restore();
  }
  function draw() {
    const rect=canvas.getBoundingClientRect();ctx.clearRect(0,0,rect.width,rect.height);
    ctx.fillStyle='#fbfcfe';ctx.fillRect(0,0,rect.width,rect.height);
    const projected=new Map(nodes.map(n=>[n,project(n)]));
    ctx.lineWidth=1.4;ctx.strokeStyle='#8793a0';
    edges.forEach(([a,b])=>{const p=projected.get(a),q=projected.get(b);ctx.beginPath();ctx.moveTo(p[0],p[1]);ctx.lineTo(q[0],q[1]);ctx.stroke();});
    points=[];
    nodes.slice().sort((a,b)=>projected.get(a)[2]-projected.get(b)[2]).forEach(node=>{
      const p=projected.get(node);
      if(node.depth===0){
        const rootRadius=14*p[2];
        ctx.beginPath();ctx.fillStyle='#17212b';ctx.arc(p[0],p[1],rootRadius,0,Math.PI*2);ctx.fill();
        ctx.beginPath();ctx.strokeStyle='#fff';ctx.lineWidth=2;ctx.arc(p[0],p[1],rootRadius*.55,0,Math.PI*2);ctx.stroke();
        points.push({x:p[0],y:p[1],r:rootRadius+5,root:true,path:node.path});
      }
      if(node.items && node.items.length){
        node.items.forEach((item,index)=>{
          const radius=(8+item.sizeRatio*18)*p[2];
          const x=p[0]+(index-(node.items.length-1)/2)*radius*2.3;
          const id=String(item.dirid);
          const isCopied=copiedIds.has(id);
          drawShape(x,p[1],radius,item.shape,isCopied?'#999999':item.color,copyBuffer.has(id),isCopied);
          ctx.fillStyle='#17212b';ctx.font='11px sans-serif';ctx.textAlign='center';ctx.fillText(id.padStart(7,'0'),x,p[1]+radius+12);
          points.push({x,y:p[1],r:radius+4,item,path:node.path});
        });
      }
      if(node.depth!==0&&(!node.items || !node.items.length)){
        ctx.beginPath();ctx.fillStyle='#aab4bf';ctx.arc(p[0],p[1],4*p[2],0,Math.PI*2);ctx.fill();
        ctx.fillStyle='#56616c';ctx.font='10px sans-serif';ctx.textAlign='center';ctx.fillText(node.name,p[0],p[1]-7);
      }
    });
  }
  canvas.addEventListener('pointerdown',e=>{drag=[e.clientX,e.clientY];dragged=false;canvas.setPointerCapture(e.pointerId);});
  canvas.addEventListener('pointermove',e=>{
    const rect=canvas.getBoundingClientRect();
    if(drag){if(Math.abs(e.clientX-drag[0])+Math.abs(e.clientY-drag[1])>3)dragged=true;yaw+=(e.clientX-drag[0])*.009;pitch=Math.max(-1.2,Math.min(1.2,pitch+(e.clientY-drag[1])*.009));drag=[e.clientX,e.clientY];draw();return;}
    const x=e.clientX-rect.left,y=e.clientY-rect.top;
    const hit=points.slice().reverse().find(p=>(p.x-x)**2+(p.y-y)**2<=p.r*p.r);
    if(hit){
      if(hit.root){tip.textContent=hit.path;tip.style.fontSize='18px';tip.style.fontWeight='700';tip.style.padding='.8rem 1rem';}
      else {tip.textContent=`${Number(hit.item.hostid)||0}:${hit.path}\ndirid ${String(hit.item.dirid).padStart(7,'0')}\n${hit.item.sizeLabel}`;tip.style.fontSize='';tip.style.fontWeight='';tip.style.padding='';}
      tip.style.display='block';tip.style.left=`${Math.min(rect.width-280,x+14)}px`;tip.style.top=`${Math.max(0,y+14)}px`;
    }
    else tip.style.display='none';
  });
  canvas.addEventListener('click',e=>{
    if(!isFinal)return;
    if(dragged){dragged=false;return;}
    const rect=canvas.getBoundingClientRect(),x=e.clientX-rect.left,y=e.clientY-rect.top;
    const hit=points.slice().reverse().find(p=>p.item&&(p.x-x)**2+(p.y-y)**2<=p.r*p.r);
    if(!hit)return;
    const id=String(hit.item.dirid);
    if(copyBuffer.has(id))copyBuffer.delete(id);
    else copyBuffer.set(id,{dirid:id,path:hit.path,groupId:hit.item.groupId||0});
    saveCopyState();draw();
  });
  function fallbackCopy(text){
    const field=document.createElement('textarea');field.value=text;field.style.position='fixed';field.style.opacity='0';
    document.body.appendChild(field);field.select();const ok=document.execCommand('copy');field.remove();
    if(!ok)throw new Error('Zwischenablage nicht verfügbar');
  }
  document.getElementById('tree-copy').addEventListener('click',async()=>{
    if(!isFinal){copyState.textContent='Copypaste ist erst im finalen HTML-Stand verfügbar.';return;}
    const idsToCopy=Array.from(new Set([...copiedIds.keys(),...copyBuffer.keys()]));
    if(!idsToCopy.length){copyState.textContent='Keine dirids ausgewählt';return;}
    const entries=Array.from(copyBuffer.values());
    const grouped=new Map();
    idsToCopy.forEach(id=>{
      const pending=copyBuffer.get(id),groupId=(pending&&pending.groupId)?pending.groupId:(copiedIds.has(id)?copiedIds.get(id):(groupById.get(id)||0));
      if(!grouped.has(groupId))grouped.set(groupId,[]);
      grouped.get(groupId).push(id);
    });
    const text=['listof_files_job__JOBID__',...Array.from(grouped.entries())
      .sort((a,b)=>Number(a[0])-Number(b[0]))
      .map(([groupId,ids])=>`${groupId}, ${ids.sort((a,b)=>Number(a)-Number(b)).map(id=>String(id).padStart(7,'0')).join(' ')}`)
    ].join('\n');
    try{
      if(navigator.clipboard&&navigator.clipboard.writeText)await navigator.clipboard.writeText(text);
      else fallbackCopy(text);
      entries.forEach(item=>{const id=String(item.dirid);copiedIds.set(id,item.groupId||groupById.get(id)||0);copyBuffer.delete(id);});
      saveCopyState();draw();
    }catch(_e){
      try{
        fallbackCopy(text);
        entries.forEach(item=>{const id=String(item.dirid);copiedIds.set(id,item.groupId||groupById.get(id)||0);copyBuffer.delete(id);});
        saveCopyState();draw();
      }catch(_fallbackError){copyState.textContent='Kopieren fehlgeschlagen; bitte Browser-Zwischenablage erlauben.';}
    }
  });
  canvas.addEventListener('pointerup',()=>{drag=null;});
  canvas.addEventListener('pointerleave',()=>{if(!drag)tip.style.display='none';});
  canvas.addEventListener('wheel',e=>{e.preventDefault();zoom=Math.max(.45,Math.min(2.5,zoom*(e.deltaY<0?1.1:.9)));draw();},{passive:false});
  document.getElementById('tree-first').addEventListener('click',()=>setPage(0));
  document.getElementById('tree-prev').addEventListener('click',()=>setPage(currentPage-1));
  document.getElementById('tree-next').addEventListener('click',()=>setPage(currentPage+1));
  document.getElementById('tree-last').addEventListener('click',()=>setPage(pages.length-1));
  window.addEventListener('resize',resize);
  resize();
  saveCopyState();
  if(!isFinal)copyState.textContent='Auswahl und Copypaste erst im finalen HTML-Stand verfügbar.';
  const savedPage=Number((location.hash.match(/page=(\d+)/)||[])[1]||0);
  setPage(savedPage);
})();
</script></section>""".replace("__JOBID__", str(jobid)).replace(
            "__FINAL__", "true" if final else "false"
        ).replace(
            "__TREE_DATA__", safe_data
        )
        markup = markup.replace("__HOST_LABEL__", label)
        if id_suffix:
            unique_ids = (
                "tree-root tree-page tree-first tree-prev tree-next tree-last tree-canvas "
                "tree-tip tree-sort tree-copy tree-copy-state tree-data sort-option-a sort-option-b"
            ).split()
            for item in unique_ids:
                markup = markup.replace(
                    f'id="{item}"', f'id="{item}-{id_suffix}"'
                )
                markup = markup.replace(
                    f"getElementById('{item}')",
                    f"getElementById('{item}-{id_suffix}')",
                )
            markup = markup.replace(
                "#tree-canvas{", f"#tree-canvas-{id_suffix}{{"
            )
        return markup


    def _render_doubles_progress_html(
        self, jobid, status, total, checked, data, tree_data=None,
        interrupted=False, auto_refresh=True,
    ):
        """Render a self-refreshing HTML report from the current cached data."""
        complete = status == "closed" and checked >= total
        percentage = 100 if complete else min(
            99, int(checked * 100 / total) if total else 0
        )
        message = "Fortschrittsanzeige mit Ctrl-C beendet; Cache kann veraltet sein." if interrupted else (
            "Unvollständige Prüfung oder Fehler." if status in ("error", "stopped", "paused")
            or (status == "closed" and checked < total) else ""
        )
        if isinstance(tree_data, dict) and any(isinstance(key, int) for key in tree_data):
            merged = {order: [] for order in ('byFileSize', 'byGroupSize')}
            for host, host_tree in sorted(tree_data.items()):
                for order in merged:
                    merged[order].extend(dict(item, hostid=host) for item in host_tree.get(order, []))
            tree_data = merged
        hosts_by_dirid = {}
        for item in (tree_data or {}).get('byFileSize', []):
            hosts_by_dirid.setdefault(item['dirid'], set()).add(item.get('hostid', 0))
        rows = []
        for group_id, directories in data:
            first_size = directories[0][2] if directories else None
            formatted_size = (
                self._human_readable_size(first_size)
                if first_size is not None else "unbekannt"
            )
            dirids = ", ".join(
                '<span class="file-dirid" data-dirid="{}" title="{}">{:07d}</span>'.format(
                    dirid, html.escape((','.join(map(str, sorted(hosts_by_dirid.get(dirid, ())))) + ':'
                                   if hosts_by_dirid.get(dirid) else '') + (root_path or ""), quote=True), dirid
                )
                for dirid, root_path, _size in directories
            )
            hostids = sorted({host for dirid, *_ in directories for host in hosts_by_dirid.get(dirid, ())})
            host_text = ', '.join(map(str, hostids)) or '-'
            rows.append(
                f'<tr class="file-group-row"><td class="file-group-id" '
                f'data-group-id="{group_id}">{group_id}</td>'
                f'<td data-sort="{first_size if first_size is not None else -1}">{formatted_size}</td>'
                f"<td>{host_text}</td><td>{dirids}</td></tr>"
            )
        table_rows = "\n".join(rows) or '<tr><td colspan="4">Keine bestätigten Duplikate im Cache.</td></tr>'
        refresh_meta = '<meta http-equiv="refresh" content="60">' if auto_refresh else ""
        refresh_text = (
            "Diese Seite aktualisiert sich alle 60 Sekunden. Duplikatdaten stammen aus dem Cache."
            if auto_refresh else "Binarycheck beendet. Dies ist der abschließende HTML-Stand."
        )
        page = """<!doctype html>
<html lang="de"><head><meta charset="utf-8">
{refresh_meta}
<title>Binarycheck {jobid}</title>
<style>body{{font:16px system-ui,sans-serif;margin:2rem}} table{{border-collapse:collapse;width:100%}}
table#files-table{{margin-top:8rem}}
th,td{{border:1px solid #bbb;padding:.5rem;text-align:left}} th{{position:sticky;top:0;background:#eee}}
td:nth-child(4) span{{border-bottom:1px dotted;cursor:help;font-family:monospace;margin-right:.35rem}}
.selected-dirid{{color:#999;text-decoration:line-through}}
th:first-child,td:first-child{{width:8ch;min-width:8ch;max-width:8ch;white-space:nowrap}}
.note{{color:#9b3b00}}</style></head>
<body><h1>Binarycheck Job {jobid}</h1>
<p><a href="#files-table">Zur Tabelle</a> &nbsp; | &nbsp;
<a href="#first-active-group">Zur ersten aktivierten Gruppe in Tabelle</a></p>
<p>Status: <strong>{status}</strong> — {percentage}% ({checked}/{total} geprüft)</p>
<p>{refresh_text}</p>
<p class="note">{message}</p>
<!--TREE_VISUALIZATION-->
<table id="files-table"><thead><tr><th tabindex="0" role="button" aria-sort="none">Gruppe</th><th tabindex="0" role="button" aria-sort="none">Größe des ersten Verzeichnisses</th><th tabindex="0" role="button" aria-sort="none">Hostids</th><th tabindex="0" role="button" aria-sort="none">dirids (Hover zeigt Ordnerpfad)</th></tr></thead>
<tbody>{table_rows}</tbody></table>
<script>
(() => {{
 const table=document.getElementById('files-table'), body=table.tBodies[0];
 const headers=Array.from(table.tHead.rows[0].cells);
 const compare=new Intl.Collator(undefined,{{numeric:true}});
 headers.forEach((header,index)=>{{
  function sort(){{
   const descending=header.getAttribute('aria-sort')==='ascending';
   headers.forEach(h=>h.setAttribute('aria-sort','none'));
   header.setAttribute('aria-sort',descending?'descending':'ascending');
   Array.from(body.querySelectorAll('.file-group-row')).sort((a,b)=>{{
    const av=a.cells[index].dataset.sort ?? a.cells[index].textContent;
    const bv=b.cells[index].dataset.sort ?? b.cells[index].textContent;
    const result=index===1 ? Number(av)-Number(bv) : compare.compare(av,bv);
    return descending ? -result : result;
   }}).forEach(row=>body.appendChild(row));
   document.dispatchEvent(new Event('file-table-sorted'));
  }}
  header.addEventListener('click',sort);
  header.addEventListener('keydown',event=>{{if(event.key==='Enter'||event.key===' '){{event.preventDefault();sort();}}}});
 }});
}})();
</script>
</body></html>""".format(
            jobid=jobid,
            status=html.escape(str(status)),
            percentage=percentage,
            checked=checked,
            total=total,
            message=html.escape(message),
            table_rows=table_rows,
            refresh_meta=refresh_meta,
            refresh_text=refresh_text,
        )
        tree_markup = (self._render_3d_tree(tree_data, jobid, final=not auto_refresh)
                       if tree_data is not None else '')
        return page.replace("<!--TREE_VISUALIZATION-->", tree_markup)


    def show_doubles_tree(self, jobid=None):
        return self.show_doubles_progress_html(
            jobid, report_prefix='doubles-tree-snapshot', tree_only=True, snapshot=True)

    def show_doubles_tree_html(self, jobid=None):
        """Report all directory IDs per nested hash group from saved deep results."""
        return self.show_doubles_progress_html(
            jobid, report_prefix="doubles-tree", tree_only=True
        )

    def show_doubles_progress_html(
        self, jobid=None, *, report_prefix="doubles-progress", tree_only=False, snapshot=False
    ):
        """Write a live, auto-refreshing HTML view of a binarycheck job."""
        source_jobid = jobid
        all_available = snapshot and jobid is None
        progress = self.database.get_double_job_progress(jobid)
        if progress is None:
            print("Keine gespeicherten Binarycheck-Ergebnisse für diesen Bericht vorhanden."
                  if snapshot else "need to start proess with --doubles-letcheck")
            return
        jobid = progress[0]
        batch = self.database.get_double_job_batch(None if all_available else jobid)
        jobids = [row[0] for row in batch] or [jobid]
        if all_available:
            jobid = "all"

        def read_progress():
            states = [self.database.get_double_job_progress(item) for item in jobids]
            if any(item is None for item in states):
                return None
            statuses = [item[1] for item in states]
            if all(value == "closed" for value in statuses):
                status = "closed"
            elif "error" in statuses:
                status = "error"
            elif "stopped" in statuses:
                status = "stopped"
            elif "paused" in statuses:
                status = "paused"
            elif "active" in statuses:
                status = "active"
            else:
                status = statuses[0]
            return (
                jobid, status,
                sum(item[2] for item in states),
                sum(item[3] for item in states),
            )

        def read_rows():
            if tree_only:
                if snapshot:
                    return self.database.list_doubles_tree_snapshot_rows(source_jobid if all_available else jobid)
                return self._bottommost_duplicate_tree_rows(self.database.list_deep_double_tree_rows(jobid))
            return [
                row for binary_jobid in jobids
                for row in self.database.list_verified_double_pairs(binary_jobid)
            ]

        def build_tree_cache(rows):
            hostids = {int(row[1]) for row in batch if len(row) > 1}
            hostids.update(int(row[12]) if len(row) > 12 and row[12] is not None else 0
                           for row in rows)
            hostids.update(int(row[13]) for row in rows if len(row) > 13)
            # Share hash-group numbers between the table and every host panel.
            hash_ids = ({hash_: index for index, hash_ in enumerate(sorted({
                row[2] for row in rows
            }), start=1)} if tree_only else None)

            def render_rows(subset, host=None):
                return self._largest_duplicate_directories(subset, hash_group_ids=hash_ids, hostid_filter=host)

            if len(hostids) > 1 or (tree_only and hostids):
                return {host: render_rows([
                    row for row in rows
                    if (int(row[12]) if len(row) > 12 and row[12] is not None else 0) == host
                    or (len(row) > 13 and int(row[13]) == host)
                ], host) for host in sorted(hostids)}
            return render_rows(rows)

        progress = read_progress()
        if progress is None:
            print("Binarycheck-Job wurde nicht gefunden.")
            return
        jobid, status, total, checked = progress
        rows_cache = read_rows()
        rows_state = progress
        data_cache = self._verified_pairs_html_data(rows_cache, group_across_hosts=tree_only)
        tree_cache = build_tree_cache(rows_cache)
        cache_updated_at = time.monotonic()
        html_path = Path.cwd() / f"{report_prefix}-{jobid}.html"
        print(
            f"Der Fortschritt kann in folgender HTML-Datei überwacht werden: "
            f"{html_path}",
            flush=True,
        )

        def write_page(interrupted=False, final=False):
            content = self._render_doubles_progress_html(
                jobid, status, total, checked, data_cache, tree_cache,
                interrupted, auto_refresh=not final,
            )
            if snapshot:
                content = content.replace('Binarycheck beendet. Dies ist der abschließende HTML-Stand.',
                                          'Momentaufnahme vorhandener Ergebnisse; laufende Vergleiche können unvollständig sein.')
            if tree_only:
                content = content.replace(
                    "</h1>",
                    "</h1><p>Verschachtelte Hash-Kandidaten aus tmp_deepdoubles_result: "
                    "alle dirids je Hash-Gruppe mit level &gt; 0. "
                    "Es werden vorhandene positive Prüfergebnisse ausgewertet; "
                    "nicht bestätigte Bereiche bleiben außen vor. Die Liste kann unvollständig sein. "
                    "Dies ist keine zusätzliche byteweise Bestätigung.</p>",
                    1,
                ).replace(
                    "Keine bestätigten Duplikate im Cache.",
                    "Keine verschachtelten Kandidaten im bisher bestätigten Teilbestand gefunden.",
                )
            temporary_path = html_path.with_suffix(".html.tmp")
            temporary_path.write_text(content, encoding="utf-8")
            os.replace(str(temporary_path), str(html_path))

        if snapshot:
            write_page(final=True)
            print('Snapshot aus vorhandenen Ergebnissen erstellt; keine neuen Vergleiche gestartet.')
            return html_path

        while True:
            if status == "closed":
                if tree_only and rows_state != progress:
                    rows_cache = read_rows()
                    data_cache = self._verified_pairs_html_data(rows_cache, group_across_hosts=tree_only)
                    tree_cache = build_tree_cache(rows_cache)
                write_page(final=True)
                incomplete = checked < total
                percentage = 100 if not incomplete else min(
                    99, int(checked * 100 / total) if total else 0
                )
                sys.stdout.write(
                    f"\r\x1b[2KJob {jobid}: {percentage}% "
                    f"({checked}/{total} geprüft)\n"
                )
                sys.stdout.flush()
                if incomplete:
                    print("there were some errors. incomplete check")
                if not tree_only:
                    self._list_verified_duplicate_files(jobid, rows=rows_cache)
                return
            write_page()
            if status in ("error", "stopped", "paused"):
                percentage = min(99, int(checked * 100 / total) if total else 0)
                sys.stdout.write(
                    f"\r\x1b[2KJob {jobid}: {percentage}% "
                    f"({checked}/{total} geprüft; {status})\n"
                )
                sys.stdout.flush()
                print("there were some errors. incomplete check")
                if not tree_only:
                    self._list_verified_duplicate_files(jobid, rows=rows_cache)
                return
            percentage = int(checked * 100 / total) if total else 0
            if status == "active":
                line = f"Job {jobid}: {percentage}% ({checked}/{total} geprüft)"
            else:
                line = f"Job {jobid}: wartet auf Server ({status})"
            sys.stdout.write(f"\r\x1b[2K{line}")
            sys.stdout.flush()
            try:
                time.sleep(20)
                progress = read_progress()
                if progress is not None and time.monotonic() - cache_updated_at >= 90:
                    rows_cache = read_rows()
                    rows_state = progress
                    data_cache = self._verified_pairs_html_data(rows_cache, group_across_hosts=tree_only)
                    tree_cache = build_tree_cache(rows_cache)
                    cache_updated_at = time.monotonic()
            except KeyboardInterrupt:
                write_page(interrupted=True)
                percentage = min(99, int(checked * 100 / total) if total else 0)
                sys.stdout.write(
                    f"\r\x1b[2KJob {jobid}: {percentage}% "
                    f"({checked}/{total} geprüft; Fortschrittsanzeige abgebrochen)\n"
                )
                sys.stdout.flush()
                print("there were some errors. incomplete check")
                print("Ausgabe aus Cache; Stand kann veraltet sein.")
                if not tree_only:
                    self._list_verified_duplicate_files(jobid, rows=rows_cache)
                return
            if progress is None:
                sys.stdout.write(f"\r\x1b[2KJob {jobid} wurde nicht gefunden.\n")
                sys.stdout.flush()
                status = "missing"
                write_page()
                return
            jobid, status, total, checked = progress

