#!/usr/bin/env python3
"""
StageAgent - 部署在业务服务器上的轻量 HTTP 服务。
接收中心服务的同步请求，本地查询数据库，写入 SQL Server 中心表。

使用:
  python3 stage_agent.py              # 默认监听 5100 端口
  python3 stage_agent.py 5200         # 指定端口

依赖:
  pip install pymysql pymssql
"""

import json
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler


class SyncHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/health":
            self._json_response(200, {"status": "ok"})
            return
        self._json_response(404, {"detail": "not found"})

    def do_POST(self):
        if self.path != "/sync":
            self._json_response(404, {"detail": "not found"})
            return

        try:
            body = self._read_body()
            result = run_sync(body)
            self._json_response(200, result)
        except Exception as e:
            self._json_response(500, {"detail": str(e)})

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        return json.loads(raw)

    def _json_response(self, code, data):
        payload = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {args[0]}")


# ─── 同步逻辑 ────────────────────────────────────────────────

def run_sync(body):
    """
    body 格式:
    {
      "serverName": "xxx",
      "source": { "databaseType": "MySQL", "host": "localhost", "port": 3306, "username": "", "password": "" },
      "target": { "host": "", "port": 1433, "databaseName": "", "username": "", "password": "" }
    }
    """
    server_name = body["serverName"]
    source = body["source"]
    target = body["target"]
    db_type = source.get("databaseType", "MySQL").lower()

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] 同步开始 - {server_name} (源: {db_type})")

    if db_type == "mysql":
        databases = find_mysql_databases(source)
        all_records = []
        for db_name in databases:
            records = query_mysql_database(source, db_name, server_name)
            all_records.extend(records)
    else:
        databases = find_sqlserver_databases(source)
        all_records = []
        for db_name in databases:
            records = query_sqlserver_database(source, db_name, server_name)
            all_records.extend(records)

    if all_records:
        write_to_central(target, server_name, all_records)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] 同步完成 - {len(databases)} 个库, {len(all_records)} 条记录")

    return {
        "serverName": server_name,
        "databases": len(databases),
        "records": len(all_records),
    }


# ─── MySQL 源 ────────────────────────────────────────────────

def find_mysql_databases(source):
    import pymysql
    conn = pymysql.connect(
        host=source["host"], port=source.get("port", 3306),
        user=source["username"], password=source["password"],
        charset="utf8mb4", connect_timeout=20,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys')
                  AND table_name IN ('mgt_exam_organize','mgt_exam_step')
                GROUP BY table_schema
                HAVING COUNT(DISTINCT table_name) = 2
            """)
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def query_mysql_database(source, db_name, server_name):
    import pymysql
    conn = pymysql.connect(
        host=source["host"], port=source.get("port", 3306),
        user=source["username"], password=source["password"],
        database=db_name, charset="utf8mb4", connect_timeout=20,
    )
    try:
        now = datetime.now()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.name, b.name, b.start_date, b.end_date
                FROM mgt_exam_organize a
                JOIN mgt_exam_step b ON a.id = b.exam_id
                ORDER BY b.start_date ASC
            """)
            rows = cur.fetchall()
        return _build_records(rows, now, server_name, db_name, "MySQL")
    finally:
        conn.close()


# ─── SQL Server 源 ───────────────────────────────────────────

def find_sqlserver_databases(source):
    import pymssql
    conn = pymssql.connect(
        server=source["host"], port=source.get("port", 1433),
        user=source["username"], password=source["password"],
        database="master", login_timeout=60, charset="utf8",
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name FROM sys.databases
                WHERE state_desc = 'ONLINE'
                  AND name NOT IN ('master','model','msdb','tempdb')
                ORDER BY name
            """)
            all_dbs = [row[0] for row in cur.fetchall()]

        matching = []
        for db_name in all_dbs:
            escaped = db_name.replace("]", "]]")
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) FROM [{escaped}].sys.tables
                    WHERE name IN ('EI_ExamTreeDesc','web_SR_CodeItem','WEB_SR_SetTime')
                """)
                if cur.fetchone()[0] == 3:
                    matching.append(db_name)
        return matching
    finally:
        conn.close()


def query_sqlserver_database(source, db_name, server_name):
    import pymssql
    escaped = db_name.replace("]", "]]")
    conn = pymssql.connect(
        server=source["host"], port=source.get("port", 1433),
        user=source["username"], password=source["password"],
        database=db_name, login_timeout=60, charset="utf8",
    )
    try:
        now = datetime.now()
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT A.Code, A.NAME, B.Description, C.KDate, C.ZDate
                FROM [{escaped}].[dbo].[EI_ExamTreeDesc] A
                JOIN [{escaped}].[dbo].[WEB_SR_SetTime] C ON A.Code = C.ExamSort
                JOIN [{escaped}].[dbo].[web_SR_CodeItem] B ON B.Codeid = 'WT' AND B.Code = C.Kind
                WHERE A.CodeLen = '2' AND C.Kind <> '06'
                ORDER BY C.KDate ASC
            """)
            rows = cur.fetchall()
        return _build_records(rows, now, server_name, db_name, "SQL Server")
    finally:
        conn.close()


# ─── 公共 ────────────────────────────────────────────────────

def _build_records(rows, now, server_name, db_name, db_type):
    records = []
    for row in rows:
        exam_code = str(row[0]).strip() if row[0] else ""
        project_name = (row[1] or "").strip()
        stage_name = (row[2] or "").strip()
        if not exam_code or not project_name or not stage_name:
            continue
        start_time, end_time = row[3], row[4]
        status = "即将开始" if now < start_time else ("已经结束" if now > end_time else "正在进行")
        records.append({
            "server_name": server_name, "database_name": db_name,
            "database_type": db_type, "exam_code": exam_code,
            "project_name": project_name, "stage_name": stage_name,
            "start_time": start_time, "end_time": end_time, "status": status,
        })
    return records


# ─── 写入中心表 ──────────────────────────────────────────────

def write_to_central(target, server_name, records):
    import pymssql
    conn = pymssql.connect(
        server=target["host"], port=target.get("port", 1433),
        user=target["username"], password=target["password"],
        database=target["databaseName"], login_timeout=20, charset="utf8",
    )
    try:
        # 按 database_name 分组
        groups = {}
        for r in records:
            groups.setdefault(r["database_name"], []).append(r)

        with conn.cursor() as cur:
            for db_name, group in groups.items():
                cur.execute(
                    "DELETE FROM dbo.project_stage_summary "
                    "WHERE source_server_name = %s AND source_database_name = %s",
                    (server_name, db_name),
                )

                seen = set()
                synced_at = datetime.now()
                for r in group:
                    key = (r["exam_code"], r["stage_name"], r["start_time"], r["end_time"])
                    if key in seen:
                        continue
                    seen.add(key)
                    cur.execute(
                        "INSERT INTO dbo.project_stage_summary "
                        "(source_server_name, source_database_name, source_database_type, exam_code, "
                        "project_name, stage_name, stage_start_time, stage_end_time, stage_status, "
                        "registration_count, admission_ticket_count, synced_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (r["server_name"], r["database_name"], r["database_type"], r["exam_code"],
                         r["project_name"], r["stage_name"], r["start_time"], r["end_time"], r["status"],
                         0, 0, synced_at),
                    )
        conn.commit()
    finally:
        conn.close()


# ─── 启动 ────────────────────────────────────────────────────

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5100
    server = HTTPServer(("0.0.0.0", port), SyncHandler)
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] StageAgent 已启动，监听端口 {port}")
    print(f"  健康检查: GET  http://localhost:{port}/health")
    print(f"  触发同步: POST http://localhost:{port}/sync")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
