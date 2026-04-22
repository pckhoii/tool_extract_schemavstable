import sys
import re
from pathlib import Path

import pandas as pd
from PyQt6.QtCore import QAbstractTableModel, QModelIndex, QObject, QRegularExpression, QThread, Qt, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QFontMetricsF, QKeySequence, QTextCharFormat, QSyntaxHighlighter
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSplitter,
    QTableView,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from find_jobs_by_table import apply_job_filter_to_outputs, generate_outputs, generate_outputs_by_job

try:
    import sqlparse
except Exception:
    sqlparse = None


class RunWorker(QObject):
    finished = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self, path: Path, schema: str, table: str, job_kw: str, search_mode: str):
        super().__init__()
        self.path = path
        self.schema = schema
        self.table = table
        self.job_kw = job_kw
        self.search_mode = search_mode

    def run(self):
        try:
            has_table_input = bool(self.schema and self.table)
            has_job_input = bool(self.job_kw)

            if has_table_input:
                target = f"{self.schema}.{self.table}"
                result = generate_outputs(self.path, target, search_mode=self.search_mode)
                if has_job_input:
                    result = apply_job_filter_to_outputs(*result, job_name_keyword=self.job_kw, target_table=target)
            elif has_job_input:
                result = generate_outputs_by_job(self.path, self.job_kw)
            else:
                raise ValueError("Nhap schema+table hoac nhap job name de chay.")
            self.finished.emit(result)
        except Exception as e:
            self.failed.emit(str(e))


class DataFrameModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame | None = None):
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()

    def set_dataframe(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df.copy()
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._df.index)

    def columnCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._df.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or role != Qt.ItemDataRole.DisplayRole:
            return None
        value = self._df.iat[index.row(), index.column()]
        if pd.isna(value):
            return ""
        return str(value)

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            return str(self._df.columns[section]) if section < len(self._df.columns) else ""
        return str(section + 1)


class CopyableTableView(QTableView):
    def keyPressEvent(self, event):
        if event.matches(QKeySequence.StandardKey.Copy):
            self.copy_selection_to_clipboard()
            return
        super().keyPressEvent(event)

    @staticmethod
    def _to_tsv_cell(value: str) -> str:
        # Preserve real newlines while keeping a single logical cell in TSV.
        text = value.replace("\r\n", "\n").replace("\r", "\n")
        if any(ch in text for ch in ['"', "\t", "\n"]):
            text = '"' + text.replace('"', '""') + '"'
        return text

    def copy_selection_to_clipboard(self):
        model = self.model()
        if model is None:
            return

        indexes = self.selectionModel().selectedIndexes()
        if not indexes:
            return

        indexes = sorted(indexes, key=lambda i: (i.row(), i.column()))
        min_row = min(i.row() for i in indexes)
        max_row = max(i.row() for i in indexes)
        min_col = min(i.column() for i in indexes)
        max_col = max(i.column() for i in indexes)

        selected = {(i.row(), i.column()) for i in indexes}
        lines = []
        for r in range(min_row, max_row + 1):
            row_vals = []
            for c in range(min_col, max_col + 1):
                if (r, c) in selected:
                    val = model.data(model.index(r, c), Qt.ItemDataRole.DisplayRole)
                    row_vals.append("" if val is None else self._to_tsv_cell(str(val)))
                else:
                    row_vals.append("")
            lines.append("\t".join(row_vals))

        QApplication.clipboard().setText("\n".join(lines))


class SqlSyntaxHighlighter(QSyntaxHighlighter):
    def __init__(self, parent):
        super().__init__(parent)
        self.rules = []
        self.target_patterns = []

        keyword_format = QTextCharFormat()
        keyword_format.setForeground(QColor("#4B7BEC"))
        keyword_format.setFontWeight(QFont.Weight.Bold)
        keywords = [
            "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL",
            "ON", "GROUP", "BY", "ORDER", "HAVING", "UNION", "ALL", "DISTINCT", "WITH",
            "AS", "CASE", "WHEN", "THEN", "ELSE", "END", "AND", "OR", "NOT", "IN",
            "IS", "NULL", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "MERGE",
            "OVER", "PARTITION", "TRUNC", "TO_DATE",
        ]
        for kw in keywords:
            self.rules.append((QRegularExpression(rf"\b{kw}\b", QRegularExpression.PatternOption.CaseInsensitiveOption), keyword_format))

        function_format = QTextCharFormat()
        function_format.setForeground(QColor("#6C5CE7"))
        self.rules.append((QRegularExpression(r"\b[A-Za-z_][A-Za-z0-9_]*(?=\s*\()"), function_format))

        string_format = QTextCharFormat()
        string_format.setForeground(QColor("#2E7D32"))
        self.rules.append((QRegularExpression(r"'([^']|'')*'"), string_format))

        number_format = QTextCharFormat()
        number_format.setForeground(QColor("#B26A00"))
        self.rules.append((QRegularExpression(r"\b\d+(?:\.\d+)?\b"), number_format))

        comment_format = QTextCharFormat()
        comment_format.setForeground(QColor("#8B97A8"))
        comment_format.setFontItalic(True)
        self.rules.append((QRegularExpression(r"--[^\n]*"), comment_format))

        self.target_format = QTextCharFormat()
        self.target_format.setForeground(QColor("#1f2937"))
        self.target_format.setBackground(QColor("#ffe08a"))
        self.target_format.setFontWeight(QFont.Weight.Bold)

    def set_target_table(self, schema: str, table: str):
        schema = (schema or "").strip()
        table = (table or "").strip()

        if not table:
            self.target_patterns = []
            return

        def _identifier_variants(name: str) -> list[str]:
            return [name, f'"{name}"', f'`{name}`', f'[{name}]']

        table_variants = _identifier_variants(table)
        patterns = []

        if schema:
            schema_variants = _identifier_variants(schema)
            for s in schema_variants:
                for t in table_variants:
                    patterns.append(rf"{re.escape(s)}\s*\.\s*{re.escape(t)}")
        else:
            for t in table_variants:
                patterns.append(rf"\b{re.escape(t)}\b")

        self.target_patterns = [
            QRegularExpression(p, QRegularExpression.PatternOption.CaseInsensitiveOption)
            for p in patterns
        ]

    def highlightBlock(self, text: str):
        for pattern, fmt in self.rules:
            it = pattern.globalMatch(text)
            while it.hasNext():
                m = it.next()
                self.setFormat(m.capturedStart(), m.capturedLength(), fmt)

        for pattern in self.target_patterns:
            it = pattern.globalMatch(text)
            while it.hasNext():
                m = it.next()
                self.setFormat(m.capturedStart(), m.capturedLength(), self.target_format)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Job Lookup - PyQt6")
        self.resize(1400, 850)

        self.summary_df = pd.DataFrame()
        self.select_df = pd.DataFrame()
        self.insert_df = pd.DataFrame()
        self.detail_df = pd.DataFrame()
        self.search_mode = "select_source"
        self._run_thread = None
        self._run_worker = None

        self._build_ui()
        self._apply_light_theme()

    def _apply_light_theme(self):
        self.setStyleSheet(
            """
            QWidget {
                background: #f2f6fc;
                color: #2c3e50;
            }
            QLabel {
                color: #3d4c63;
                font-weight: 600;
            }
            QLineEdit, QTextEdit {
                background: #fbfdff;
                border: 1px solid #cbd6e5;
                border-radius: 8px;
                padding: 7px 9px;
                selection-background-color: #4B7BEC;
                selection-color: #ffffff;
            }
            QComboBox {
                background: #fbfdff;
                border: 1px solid #c8d4e4;
                border-radius: 10px;
                padding: 7px 34px 7px 10px;
                color: #31445e;
                min-height: 22px;
            }
            QComboBox:hover {
                border: 1px solid #9eb3d1;
                background: #f4f8ff;
            }
            QComboBox:focus {
                border: 1px solid #4B7BEC;
                background: #ffffff;
            }
            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 28px;
                border-left: 1px solid #d6dfeb;
                background: #eef4fb;
                border-top-right-radius: 10px;
                border-bottom-right-radius: 10px;
            }
            QComboBox::down-arrow {
                width: 10px;
                height: 10px;
            }
            QComboBox QAbstractItemView {
                background: #ffffff;
                border: 1px solid #c8d4e4;
                selection-background-color: #4B7BEC;
                selection-color: #ffffff;
                padding: 4px;
                outline: 0;
            }
            QLabel#searchModeLabel {
                color: #345995;
                font-weight: 700;
            }
            QTextEdit#sqlPreview {
                background: #eef3fb;
                color: #243447;
                border: 1px solid #c4d1e3;
                font-family: Consolas, "Courier New", monospace;
                font-size: 12px;
                selection-background-color: #4B7BEC;
                selection-color: #ffffff;
            }
            QTableView {
                background: #fbfdff;
                alternate-background-color: #f3f8ff;
                border: 1px solid #c9d5e5;
                border-radius: 8px;
                gridline-color: #e2e9f3;
                selection-background-color: #4B7BEC;
                selection-color: #ffffff;
            }
            QHeaderView::section {
                background: #e7eef9;
                color: #3d4c63;
                border: none;
                border-bottom: 1px solid #c9d5e5;
                border-right: 1px solid #c9d5e5;
                padding: 6px;
                font-weight: 700;
            }
            QTabWidget::pane {
                border: 1px solid #c9d5e5;
                border-radius: 10px;
                background: #fbfdff;
            }
            QTabBar::tab {
                background: #e6edf8;
                color: #4a5d78;
                padding: 7px 14px;
                margin-right: 4px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
            }
            QTabBar::tab:selected {
                background: #fbfdff;
                color: #2d3f58;
                font-weight: 700;
            }
            QProgressBar {
                background: #e6edf8;
                border: 1px solid #c9d5e5;
                border-radius: 7px;
                min-height: 8px;
            }
            QProgressBar::chunk {
                background: #4B7BEC;
                border-radius: 6px;
            }
            QPushButton {
                border: none;
                border-radius: 8px;
                padding: 8px 14px;
                font-weight: 700;
                color: #2d3f58;
                background: #dfe7f3;
            }
            QPushButton:hover {
                background: #cfd9e8;
            }
            QPushButton:disabled {
                background: #e2e8f0;
                color: #99a7bb;
            }
            QPushButton#btnPrimary {
                background: #4B7BEC;
                color: #ffffff;
            }
            QPushButton#btnPrimary:hover {
                background: #3d6edc;
            }
            QPushButton#btnSuccess {
                background: #2f9e8f;
                color: #ffffff;
            }
            QPushButton#btnSuccess:hover {
                background: #248577;
            }
            QPushButton#btnSecondary {
                background: #5d8ddf;
                color: #ffffff;
            }
            QPushButton#btnSecondary:hover {
                background: #4c7ac9;
            }
            QSplitter::handle:vertical {
                background: #c9d5e5;
                height: 8px;
                margin: 4px 0;
                border-radius: 4px;
            }
            """
        )

    def _build_ui(self):
        root = QWidget()
        layout = QVBoxLayout(root)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        top = QHBoxLayout()
        top.setSpacing(8)
        top.addWidget(QLabel("Excel path:"))
        self.input_path = QLineEdit("etl_script_20Apr2026.xlsx")
        top.addWidget(self.input_path, 1)
        browse_btn = QPushButton("Browse")
        browse_btn.setObjectName("btnSecondary")
        browse_btn.clicked.connect(self.on_browse)
        top.addWidget(browse_btn)
        layout.addLayout(top)

        row2 = QHBoxLayout()
        row2.setSpacing(8)
        row2.addWidget(QLabel("Schema:"))
        self.schema_input = QLineEdit("EOC")
        row2.addWidget(self.schema_input)
        row2.addWidget(QLabel("Table:"))
        self.table_input = QLineEdit("FM_DEPARTMENT")
        row2.addWidget(self.table_input)
        search_mode_label = QLabel("Search mode:")
        search_mode_label.setObjectName("searchModeLabel")
        row2.addWidget(search_mode_label)
        self.search_mode_input = QComboBox()
        self.search_mode_input.addItem("SELECT FROM table", "select_source")
        self.search_mode_input.addItem("INSERT INTO table", "insert_target")
        self.search_mode_input.setMinimumWidth(210)
        row2.addWidget(self.search_mode_input)
        row2.addWidget(QLabel("Job name contains:"))
        self.job_name_input = QLineEdit("")
        self.job_name_input.setPlaceholderText("VD: ADMIN")
        row2.addWidget(self.job_name_input)

        self.run_btn = QPushButton("Run")
        self.run_btn.setObjectName("btnPrimary")
        self.run_btn.clicked.connect(self.on_run)
        row2.addWidget(self.run_btn)

        self.export_btn = QPushButton("Export Excel")
        self.export_btn.setObjectName("btnSuccess")
        self.export_btn.clicked.connect(self.on_export)
        row2.addWidget(self.export_btn)
        layout.addLayout(row2)

        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)
        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        self.progress.hide()
        layout.addWidget(self.progress)

        tabs = QTabWidget()
        self.select_model = DataFrameModel()
        self.insert_model = DataFrameModel()
        self.detail_model = DataFrameModel()

        self.select_table = CopyableTableView()
        self.select_table.setModel(self.select_model)
        self.select_table.setSortingEnabled(True)
        self.select_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        tabs.addTab(self.select_table, "Select Jobs")

        self.insert_table = CopyableTableView()
        self.insert_table.setModel(self.insert_model)
        self.insert_table.setSortingEnabled(True)
        self.insert_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        tabs.addTab(self.insert_table, "Insert Jobs")

        self.detail_table = CopyableTableView()
        self.detail_table.setModel(self.detail_model)
        self.detail_table.setSortingEnabled(True)
        self.detail_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        tabs.addTab(self.detail_table, "Detail Output")

        for tv in [self.select_table, self.insert_table, self.detail_table]:
            tv.setAlternatingRowColors(True)
            tv.setWordWrap(False)

        preview_panel = QFrame()
        preview_layout = QVBoxLayout(preview_panel)
        preview_layout.setContentsMargins(0, 0, 0, 0)
        preview_layout.setSpacing(8)

        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        copy_cell_btn = QPushButton("Copy Selected Cell")
        copy_cell_btn.setObjectName("btnSecondary")
        copy_cell_btn.clicked.connect(self.copy_current_cell)
        action_row.addWidget(copy_cell_btn)
        copy_row_btn = QPushButton("Copy Selected Row")
        copy_row_btn.setObjectName("btnSecondary")
        copy_row_btn.clicked.connect(self.copy_current_row)
        action_row.addWidget(copy_row_btn)
        action_row.addWidget(QLabel("Tip: Ctrl+C de copy vung dang chon"))
        action_row.addStretch()
        preview_layout.addLayout(action_row)

        self.cell_preview = QTextEdit()
        self.cell_preview.setObjectName("sqlPreview")
        self.cell_preview.setReadOnly(True)
        self.cell_preview.setPlaceholderText("Click vao mot o de xem full text o day...")
        self.cell_preview.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        code_font = QFont("Consolas")
        code_font.setStyleHint(QFont.StyleHint.Monospace)
        self.cell_preview.setFont(code_font)
        self.cell_preview.setTabStopDistance(QFontMetricsF(code_font).horizontalAdvance(" ") * 4)
        self.sql_highlighter = SqlSyntaxHighlighter(self.cell_preview.document())
        self.schema_input.textChanged.connect(self._refresh_target_highlight)
        self.table_input.textChanged.connect(self._refresh_target_highlight)
        self._refresh_target_highlight()
        self.cell_preview.setMinimumHeight(130)
        preview_layout.addWidget(self.cell_preview, 1)

        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(tabs)
        splitter.addWidget(preview_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        splitter.setSizes([560, 250])
        layout.addWidget(splitter, 1)

        self.select_table.clicked.connect(self.on_cell_clicked)
        self.insert_table.clicked.connect(self.on_cell_clicked)
        self.detail_table.clicked.connect(self.on_cell_clicked)

        self.setCentralWidget(root)

    def on_browse(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Excel File",
            "",
            "Excel Files (*.xlsx *.xlsm *.xls);;All Files (*)",
        )
        if path:
            self.input_path.setText(path)

    def on_run(self):
        if self._run_thread is not None:
            return

        path = Path(self.input_path.text().strip())
        schema = self.schema_input.text().strip()
        table = self.table_input.text().strip()
        job_kw = self.job_name_input.text().strip()
        self.search_mode = self.search_mode_input.currentData() or "select_source"

        if not path.exists():
            QMessageBox.warning(self, "Missing file", f"Khong tim thay file:\n{path}")
            return
        has_table_input = bool(schema and table)
        has_job_input = bool(job_kw)
        if not has_table_input and not has_job_input:
            QMessageBox.warning(
                self,
                "Missing input",
                "Vui long nhap schema+table hoac nhap Job name contains.",
            )
            return

        if has_table_input:
            target = f"{schema}.{table}"
            mode_text = "SELECT FROM" if self.search_mode == "select_source" else "INSERT INTO"
            if has_job_input:
                self.status_label.setText(
                    f"Running... mode={mode_text}, target={target}, job contains='{job_kw}'"
                )
            else:
                self.status_label.setText(f"Running... mode={mode_text}, target={target}")
        else:
            self.status_label.setText(f"Running... job contains='{job_kw}'")
        self.progress.setRange(0, 0)
        self.progress.show()
        self.run_btn.setEnabled(False)
        self.export_btn.setEnabled(False)

        self._run_thread = QThread()
        self._run_worker = RunWorker(path, schema, table, job_kw, self.search_mode)
        self._run_worker.moveToThread(self._run_thread)
        self._run_thread.started.connect(self._run_worker.run)
        self._run_worker.finished.connect(self.on_run_finished)
        self._run_worker.failed.connect(self.on_run_failed)
        self._run_worker.finished.connect(self._run_thread.quit)
        self._run_worker.failed.connect(self._run_thread.quit)
        self._run_thread.finished.connect(self._cleanup_run_thread)
        self._run_thread.start()

    def on_run_finished(self, result):
        self.summary_df, self.select_df, self.insert_df, self.detail_df = result
        schema = self.schema_input.text().strip()
        table = self.table_input.text().strip()
        job_kw = self.job_name_input.text().strip()
        job_only_mode = bool(job_kw) and not (schema and table)
        insert_target_mode = self.search_mode == "insert_target"

        self.select_model.set_dataframe(self.select_df)
        self.insert_model.set_dataframe(self.insert_df)
        self.detail_model.set_dataframe(self.detail_df)

        self.select_table.resizeColumnsToContents()
        self.insert_table.resizeColumnsToContents()
        self.detail_table.resizeColumnsToContents()

        if job_only_mode:
            self.status_label.setText(
                f"Done | Job filter mode | matched rows: {len(self.select_df)} (theo JOB_NAME contains='{job_kw}')"
            )
        elif insert_target_mode and self.insert_df.empty:
            self.status_label.setText(
                "Done | Khong tim thay INSERT match cho bang input. Kiem tra lai schema/table."
            )
        elif (not insert_target_mode) and self.select_df.empty:
            self.status_label.setText(
                "Done | Khong tim thay SELECT match cho bang input. Kiem tra lai schema/table."
            )
        else:
            mode_text = "INSERT INTO" if insert_target_mode else "SELECT FROM"
            self.status_label.setText(
                f"Done | Mode={mode_text} | SELECT rows: {len(self.select_df)} | INSERT rows: {len(self.insert_df)} | DETAIL rows: {len(self.detail_df)}"
            )
        self.cell_preview.clear()

    def on_run_failed(self, message: str):
        QMessageBox.critical(self, "Run failed", message)
        self.status_label.setText("Run failed")

    def _cleanup_run_thread(self):
        self.progress.hide()
        self.run_btn.setEnabled(True)
        self.export_btn.setEnabled(True)
        if self._run_worker is not None:
            self._run_worker.deleteLater()
        if self._run_thread is not None:
            self._run_thread.deleteLater()
        self._run_worker = None
        self._run_thread = None

    def get_active_table(self):
        for tv in [self.select_table, self.insert_table, self.detail_table]:
            if tv.hasFocus():
                return tv

        for tv in [self.select_table, self.insert_table, self.detail_table]:
            idx = tv.currentIndex()
            if idx.isValid():
                return tv
        return None

    def on_cell_clicked(self, index):
        if not index.isValid():
            self.cell_preview.clear()
            return
        tv = self.sender()
        model = tv.model()
        value = model.data(index, Qt.ItemDataRole.DisplayRole)
        header = model.headerData(index.column(), Qt.Orientation.Horizontal, Qt.ItemDataRole.DisplayRole)
        text_value = "" if value is None else str(value)
        formatted = self._format_preview_text(str(header), text_value)
        self.cell_preview.setPlainText(f"{header}\n{'=' * len(str(header))}\n{formatted}")

    def _refresh_target_highlight(self):
        if not hasattr(self, "sql_highlighter"):
            return
        schema = self.schema_input.text().strip() if hasattr(self, "schema_input") else ""
        table = self.table_input.text().strip() if hasattr(self, "table_input") else ""
        self.sql_highlighter.set_target_table(schema, table)
        self.sql_highlighter.rehighlight()

    def _format_preview_text(self, header: str, text: str) -> str:
        if not text:
            return ""

        header_upper = header.upper()
        is_sql_col = "STATEMENT" in header_upper or "SQL" in header_upper
        if is_sql_col:
            return self._format_sql_text(text)
        return text

    @staticmethod
    def _format_sql_text(text: str) -> str:
        sql = text.replace("\r\n", "\n").replace("\r", "\n").strip()
        if not sql:
            return ""

        if sqlparse is not None:
            try:
                return sqlparse.format(sql, reindent=True, keyword_case="upper")
            except Exception:
                pass

        compact = re.sub(r"\s+", " ", sql)

        keywords = [
            "INSERT INTO",
            "INSERT OVERWRITE TABLE",
            "MERGE INTO",
            "LEFT OUTER JOIN",
            "RIGHT OUTER JOIN",
            "FULL OUTER JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL JOIN",
            "GROUP BY",
            "ORDER BY",
            "UNION ALL",
            "SELECT",
            "FROM",
            "WHERE",
            "JOIN",
            "HAVING",
            "UNION",
            "VALUES",
            "SET",
            "ON",
            "AND",
            "OR",
        ]

        for kw in keywords:
            pat = re.compile(rf"\s+({re.escape(kw)})\b", flags=re.IGNORECASE)
            compact = pat.sub(lambda m: "\n" + m.group(1).upper(), compact)

        compact = re.sub(r"\s*,\s*", ",\n    ", compact)
        lines = [ln.rstrip() for ln in compact.split("\n") if ln.strip()]

        indented = []
        for ln in lines:
            up = ln.upper()
            if up.startswith(("AND ", "OR ", "ON ")):
                indented.append("    " + ln)
            elif up.startswith(("JOIN ", "LEFT JOIN ", "RIGHT JOIN ", "INNER JOIN ", "FULL JOIN ")):
                indented.append("  " + ln)
            else:
                indented.append(ln)
        return "\n".join(indented)

    def copy_current_cell(self):
        tv = self.get_active_table()
        if tv is None:
            return
        idx = tv.currentIndex()
        if not idx.isValid():
            QMessageBox.information(self, "No selection", "Chon mot o truoc khi copy.")
            return
        value = tv.model().data(idx, Qt.ItemDataRole.DisplayRole)
        QApplication.clipboard().setText("" if value is None else str(value))

    def copy_current_row(self):
        tv = self.get_active_table()
        if tv is None:
            return
        idx = tv.currentIndex()
        if not idx.isValid():
            QMessageBox.information(self, "No selection", "Chon mot dong truoc khi copy.")
            return
        model = tv.model()
        r = idx.row()
        vals = []
        for c in range(model.columnCount()):
            val = model.data(model.index(r, c), Qt.ItemDataRole.DisplayRole)
            vals.append("" if val is None else CopyableTableView._to_tsv_cell(str(val)))
        QApplication.clipboard().setText("\t".join(vals))

    def on_export(self):
        if self.summary_df.empty and self.select_df.empty and self.insert_df.empty and self.detail_df.empty:
            QMessageBox.information(self, "No data", "Chua co du lieu. Bam Run truoc.")
            return

        schema = self.schema_input.text().strip().upper() or "SCHEMA"
        table = self.table_input.text().strip().upper() or "TABLE"
        default_name = f"job_lookup_{schema}_{table}.xlsx"
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Output Excel", default_name, "Excel Files (*.xlsx)"
        )
        if not file_path:
            return

        try:
            with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
                self.summary_df.to_excel(writer, sheet_name="summary", index=False)

                if self.select_df.empty:
                    pd.DataFrame([{"INFO": "No SELECT match found"}]).to_excel(
                        writer, sheet_name="select_part", index=False
                    )
                else:
                    self.select_df.sort_values(["JOB_NAME", "SHEET_NAME", "ROW_NO_IN_SHEET"]).to_excel(
                        writer, sheet_name="select_part", index=False
                    )

                if self.insert_df.empty:
                    pd.DataFrame([{"INFO": "No related INSERT found for matched jobs"}]).to_excel(
                        writer, sheet_name="insert_part", index=False
                    )
                else:
                    self.insert_df.sort_values(["JOB_NAME", "SHEET_NAME", "ROW_NO_IN_SHEET"]).to_excel(
                        writer, sheet_name="insert_part", index=False
                    )

                if self.detail_df.empty:
                    pd.DataFrame([{"INFO": "No result rows for detail output"}]).to_excel(
                        writer, sheet_name="detail_output", index=False
                    )
                else:
                    self.detail_df.to_excel(writer, sheet_name="detail_output", index=False)
        except Exception as e:
            QMessageBox.critical(self, "Export failed", str(e))
            return

        QMessageBox.information(self, "Success", f"Da xuat file:\n{file_path}")


def main():
    app = QApplication(sys.argv)
    base_font = app.font()
    if base_font.pointSize() <= 0 and base_font.pointSizeF() <= 0:
        base_font.setPointSize(10)
        app.setFont(base_font)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
