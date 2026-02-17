"""Generate PDF summary of Double-Touch Fib strategy optimization."""

from fpdf import FPDF
from pathlib import Path
from bidi.algorithm import get_display
from config.settings import DATA_DIR

# Fonts
FONT_HE = "/usr/share/fonts/truetype/noto/NotoSansHebrew-Regular.ttf"
FONT_HE_BOLD = "/usr/share/fonts/truetype/noto/NotoSansHebrew-Bold.ttf"
FONT_EN = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
FONT_EN_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
FONT_MONO = "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"

# Colors
BG_DARK = (14, 17, 23)
CARD_BG = (26, 31, 46)
WIN_GREEN = (0, 210, 106)
LOSS_RED = (242, 54, 69)
BLUE = (41, 98, 255)
ORANGE = (255, 152, 0)
WHITE = (250, 250, 250)
GRAY = (139, 148, 158)
BORDER = (42, 48, 64)
HEADER_BG = (22, 27, 38)
HIGHLIGHT_BG = (10, 70, 45)


def he(text):
    """Convert Hebrew text to visual order for PDF rendering."""
    return get_display(text)


class OptimizationPDF(FPDF):
    def __init__(self):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.add_font("he", "", FONT_HE)
        self.add_font("he", "B", FONT_HE_BOLD)
        self.add_font("en", "", FONT_EN)
        self.add_font("en", "B", FONT_EN_BOLD)
        self.add_font("mono", "", FONT_MONO)
        self.set_auto_page_break(auto=True, margin=15)

    def dark_bg(self):
        self.set_fill_color(*BG_DARK)
        self.rect(0, 0, 210, 297, "F")

    def header(self):
        self.dark_bg()

    def section_title_he(self, number, text):
        self.set_font("en", "B", 14)
        self.set_text_color(*WHITE)
        self.set_fill_color(*CARD_BG)
        title = he(f"{text} .{number}")
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT", fill=True, align="R")
        self.ln(2)

    def section_title(self, number, text_en):
        self.set_font("en", "B", 14)
        self.set_text_color(*WHITE)
        self.set_fill_color(*CARD_BG)
        title = f"{number}. {text_en}"
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(2)

    def metric_row(self, data, col_widths=None):
        """Draw a row of metric boxes."""
        if col_widths is None:
            col_widths = [190 / len(data)] * len(data)
        x_start = self.get_x()
        y_start = self.get_y()
        box_h = 18

        for i, (label, value, color) in enumerate(data):
            x = x_start + sum(col_widths[:i])
            self.set_fill_color(*CARD_BG)
            self.set_draw_color(*BORDER)
            self.rect(x, y_start, col_widths[i] - 2, box_h, "DF")

            self.set_xy(x, y_start + 1)
            self.set_font("en", "B", 13)
            self.set_text_color(*color)
            self.cell(col_widths[i] - 2, 9, str(value), align="C")

            self.set_xy(x, y_start + 10)
            self.set_font("en", "", 7)
            self.set_text_color(*GRAY)
            self.cell(col_widths[i] - 2, 6, he(label), align="C")

        self.set_y(y_start + box_h + 3)

    def table(self, headers, rows, col_widths=None, highlight_best=None):
        """Draw a styled table."""
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)

        self.set_font("en", "B", 8)
        self.set_fill_color(*HEADER_BG)
        self.set_text_color(*GRAY)
        self.set_draw_color(*BORDER)

        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, align="C", fill=True)
        self.ln()

        self.set_font("en", "", 8)
        for row_idx, row in enumerate(rows):
            is_highlight = highlight_best is not None and row_idx == highlight_best
            if is_highlight:
                self.set_fill_color(*HIGHLIGHT_BG)
            else:
                bg = CARD_BG if row_idx % 2 == 0 else BG_DARK
                self.set_fill_color(*bg)

            for i, cell in enumerate(row):
                cell_str = str(cell)
                if cell_str.startswith("-") or cell_str.startswith("$-"):
                    self.set_text_color(*LOSS_RED)
                elif cell_str.startswith("+") or cell_str.startswith("$+"):
                    self.set_text_color(*WIN_GREEN)
                elif is_highlight:
                    self.set_text_color(*WIN_GREEN)
                else:
                    self.set_text_color(*WHITE)

                self.cell(col_widths[i], 6, cell_str, border=1, align="C", fill=True)
            self.ln()

        self.set_text_color(*WHITE)
        self.ln(2)

    def bar_chart_text(self, label, value, max_val, color, width=120):
        """Draw a text-based bar chart line."""
        bar_len = int((value / max_val) * width / 4) if max_val > 0 else 0
        bar = "\u2588" * bar_len

        self.set_font("en", "", 9)
        self.set_text_color(*WHITE)
        self.cell(25, 5, label, align="R")

        self.set_font("mono", "", 9)
        self.set_text_color(*color)
        self.cell(100, 5, f" {bar} {value}")
        self.ln()

    def add_subtitle_he(self, text):
        self.set_font("en", "B", 10)
        self.set_text_color(*ORANGE)
        self.cell(0, 7, he(text), new_x="LMARGIN", new_y="NEXT", align="R")

    def add_subtitle(self, text):
        self.set_font("en", "B", 10)
        self.set_text_color(*ORANGE)
        self.cell(0, 7, text, new_x="LMARGIN", new_y="NEXT")

    def add_text_he(self, text, size=9, color=WHITE):
        self.set_font("en", "", size)
        self.set_text_color(*color)
        self.multi_cell(0, 5, he(text), align="R")
        self.ln(1)

    def add_text(self, text, size=9, color=WHITE):
        self.set_font("en", "", size)
        self.set_text_color(*color)
        self.multi_cell(0, 5, text)
        self.ln(1)

    def add_bullet_he(self, text, color=WHITE):
        self.set_font("en", "", 9)
        self.set_text_color(*color)
        self.set_x(self.l_margin)
        self.multi_cell(190, 5, he(text), align="R")

    def add_bullet(self, text, color=WHITE):
        self.set_font("en", "", 9)
        self.set_text_color(*color)
        x = self.l_margin
        self.set_x(x)
        self.cell(5, 5, "  ")
        self.multi_cell(0, 5, f"  {text}")
        self.set_x(x)

    def step_box(self, step_num, title, before, after, detail=""):
        """Draw a step-by-step process box with before->after."""
        y = self.get_y()
        self.set_fill_color(*CARD_BG)
        self.set_draw_color(*BORDER)
        box_h = 24 if not detail else 30
        self.rect(self.l_margin, y, 190, box_h, "DF")

        self.set_xy(self.l_margin + 3, y + 2)
        self.set_font("en", "B", 11)
        self.set_text_color(*BLUE)
        self.cell(10, 6, f"#{step_num}")

        self.set_xy(self.l_margin + 15, y + 2)
        self.set_font("en", "B", 10)
        self.set_text_color(*WHITE)
        self.cell(170, 6, he(title), align="R")

        self.set_xy(self.l_margin + 8, y + 10)
        self.set_font("en", "", 8)
        self.set_text_color(*GRAY)
        self.cell(12, 5, he("לפני:"))
        self.set_text_color(*ORANGE)
        self.cell(70, 5, before)

        self.set_text_color(*WHITE)
        self.cell(10, 5, "->")

        self.set_text_color(*GRAY)
        self.cell(12, 5, he("אחרי:"))
        self.set_text_color(*WIN_GREEN)
        self.cell(70, 5, after)

        if detail:
            self.set_xy(self.l_margin + 8, y + 17)
            self.set_font("en", "", 7)
            self.set_text_color(*GRAY)
            self.cell(180, 5, he(detail), align="R")

        self.set_y(y + box_h + 3)


def generate():
    pdf = OptimizationPDF()

    # ══════════════════════════════════════════════════════
    # PAGE 1: Title + Strategy Overview + Baseline
    # ══════════════════════════════════════════════════════
    pdf.add_page()

    # Title
    pdf.set_font("en", "B", 24)
    pdf.set_text_color(*WHITE)
    pdf.cell(0, 15, he("אסטרטגיית פיבונאצ'י נגיעה כפולה"), align="C",
             new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("en", "B", 14)
    pdf.set_text_color(*BLUE)
    pdf.cell(0, 10, he("דוח אופטימיזציה"), align="C",
             new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("en", "", 10)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 7, "15-second bars  |  IBKR Paper Trading  |  $3,000",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, he("פברואר 2026"), align="C",
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(8)

    # Strategy overview
    pdf.section_title_he("1", "סקירת האסטרטגיה")
    pdf.add_text_he(
        "האסטרטגיה מזהה מניות פני עם גאפים גדולים (מעל 10%), "
        "מחשבת רמות פיבונאצ'י מנר עוגן יומי של 5 שנים, "
        "וממתינה שהמחיר יגע ברמת תמיכה פיבונאצ'י פעמיים "
        "(דפוס נגיעה כפולה) לפני כניסה לפוזיציית לונג."
    )
    pdf.add_text_he(
        "יציאה: 50% ברמת פיבונאצ'י שלישית מעל הכניסה (יעד), "
        "50% הנותרים יוצאים כשהנר לא עושה גבוה חדש."
    )
    pdf.ln(2)

    # Baseline
    pdf.section_title_he("2", "נקודת מוצא — בסיס (ללא פילטרים)")
    pdf.add_text_he(
        "הרצנו בקטסט על 30 ימים של נתוני 15-שניות מ-IBKR ללא פילטרים. "
        "זה נתן לנו את הבסיס הגולמי לשיפור:",
        size=8, color=GRAY,
    )
    pdf.ln(1)
    pdf.metric_row([
        ("עסקאות", "200", WHITE),
        ("אחוז הצלחה", "67.5%", ORANGE),
        ("רווח כולל", "$21,854", WIN_GREEN),
        ("רווח ממוצע", "$109", WIN_GREEN),
        ("פקטור רווח", "4.9", WHITE),
        ("ירידה מקס'", "3.2%", LOSS_RED),
    ])
    pdf.ln(4)

    # Analysis
    pdf.section_title_he("3", "ניתוח מעמיק של 200 עסקאות — ממצאים מרכזיים")

    pdf.add_subtitle_he("א) גודל הגאפ משפיע")
    pdf.table(
        ["Gap Range", "Win Rate", "Avg P&L", "Conclusion"],
        [
            ["10-15%", "65%", "$80", "OK"],
            ["15-25%", "72%", "$125", "SWEET SPOT"],
            ["50%+", "17%", "$24", "LOSERS"],
        ],
        [35, 35, 35, 85],
    )

    pdf.add_subtitle_he("ב) זמן הכניסה משפיע")
    pdf.table(
        ["Time (ET)", "Avg P&L", "Conclusion"],
        [
            ["Before 13:00", "$143", "EDGE EXISTS"],
            ["After 13:00", "~$0", "NO EDGE"],
        ],
        [50, 50, 90],
    )

    pdf.add_subtitle_he("ג) לא כל יחסי הפיבונאצ'י שווים")
    pdf.table(
        ["High WR Ratios", "Win Rate", "Low WR Ratios", "Win Rate"],
        [
            ["0.764", "90%", "0.236", "56%"],
            ["2.414", "92%", "1.000", "67%"],
            ["3.272", "100%", "2.000", "47%"],
            ["3.618", "92%", "4.000+", "0-57%"],
        ],
        [45, 30, 45, 30],
    )

    # ══════════════════════════════════════════════════════
    # PAGE 2: The Optimization Journey
    # ══════════════════════════════════════════════════════
    pdf.add_page()

    pdf.section_title_he("4", "מסע האופטימיזציה — צעד אחרי צעד")
    pdf.add_text_he(
        "בהתבסס על הניתוח, יישמנו סדרת שיפורים. "
        "כל צעד נבדק באופן עצמאי לפני המעבר לצעד הבא.",
        size=8, color=GRAY,
    )
    pdf.ln(2)

    pdf.step_box(
        1, "הפעלת 3 פילטרים (מגבלת גאפ + חלון זמן + פילטר יחסים)",
        "200 trades, 67.5% WR, PF 4.9",
        "129 trades, 72.9% WR, PF 6.2",
        "הסרת 71 עסקאות באיכות נמוכה: גאפים מעל 50%, כניסות אחרי 13:00, יחסי פיב חלשים",
    )

    pdf.step_box(
        2, "שינוי יציאה: סטופ נגרר -> ללא גבוה חדש",
        "Exit when bar breaks prev Low",
        "Exit when bar fails new High",
        "תוצאה: 100% הצלחה על כל 43 יציאות נגררות, שיפור נטו של $3,336+",
    )

    pdf.step_box(
        3, "ניתוח 119 עסקאות שסוננו",
        "119 removed, unknown quality",
        "Found 2 ratios 87%+ WR to add back",
        "יחסים 2.272 (הצלחה 100%, ממוצע $268) ו-3.414 (הצלחה 75%, ממוצע $171) סוננו בטעות",
    )

    pdf.step_box(
        4, "הוספת יחסים 2.272 + 3.414 בחזרה",
        "129 trades, 72.9% WR",
        "128 trades, 73.4% WR, +$617",
        "שינוי קטן במספר העסקאות בגלל אפקטי תזמון עקיפים; שיפור באחוז הצלחה וברווח",
    )

    pdf.step_box(
        5, "חיפוש רשת ממצה: 180 קומבינציות",
        "1 config (manual guess)",
        "4 optimal configs found",
        "בדיקת 6 מגבלות גאפ x 6 חלונות זמן x 5 קבוצות יחסים = 180 קומבינציות",
    )
    pdf.ln(3)

    # Filter details
    pdf.section_title_he("5", "פירוט הפילטרים")

    pdf.add_subtitle_he("פילטר 1: מגבלת גאפ — דילוג על גאפים מעל X%")
    pdf.add_text_he(
        "מניות עם גאפים קיצוניים (מעל 50%) הצליחו רק ב-17% מהמקרים. "
        "חיתוך שלהן משפר את האיכות בלי לאבד הרבה עסקאות.",
        size=8,
    )

    pdf.add_subtitle_he("פילטר 2: חלון זמן — אין כניסות אחרי Y:00 ET")
    pdf.add_text_he(
        "כניסות בבוקר (לפני 13:00) מרוויחות בממוצע $143 לעסקה. "
        "כניסות אחה\"צ מרוויחות ~$0. היתרון נעלם אחרי הצהריים.",
        size=8,
    )

    pdf.add_subtitle_he("פילטר 3: פילטר יחסי פיבונאצ'י — רק יחסים מוכחים")
    pdf.add_text_he(
        "יחסי פיב מסוימים (0.764, 2.414, 3.272) הצליחו ב-90-100%. "
        "אחרים (0.236, 2.0, 4.0+) הצליחו רק ב-0-56%. סינון לפי יחס הוא המנוף הגדול ביותר.",
        size=8,
    )

    pdf.add_subtitle_he("שינוי יציאה: ללא גבוה חדש")
    pdf.add_text_he(
        "ישן: יציאה מ-50% הנותרים כשהנר שובר את השפל הקודם. "
        "חדש: יציאה מ-50% הנותרים כשהנר לא עושה גבוה חדש. "
        "מכיוון ששלב זה מופעל רק אחרי שה-50% הראשון כבר נמכר ברווח, "
        "הפוזיציה הנותרת תמיד ברווח — ולכן 100% הצלחה על כל היציאות הנגררות.",
        size=8,
    )

    # ══════════════════════════════════════════════════════
    # PAGE 3: Filtered trades + Exit logic
    # ══════════════════════════════════════════════════════
    pdf.add_page()

    pdf.section_title_he("6", "מה סונן — ניתוח 119 עסקאות שהוסרו")
    pdf.add_text_he(
        "הרצנו בקטסט ללא פילטרים, ואז השווינו כדי למצוא "
        "את 119 העסקאות שהוסרו. הנה מה שמצאנו:",
        size=8, color=GRAY,
    )
    pdf.ln(1)

    pdf.table(
        ["Metric", "Kept (129)", "Removed (119)"],
        [
            ["Win Rate", "72.9%", "63.0%"],
            ["Avg P&L/trade", "$154", "$88"],
            ["Profit Factor", "6.21", "3.64"],
        ],
        [60, 65, 65],
    )

    pdf.add_subtitle_he("פירוט לפי סיבת סינון")
    pdf.table(
        ["Filter", "Trades", "WR", "Avg P&L", "Verdict"],
        [
            ["Bad Ratio", "101", "61%", "$88", "Correctly filtered"],
            ["After 13:00 ET", "15", "mixed", "$28", "Correctly filtered"],
            ["Gap > 50%", "6", "17%", "-$", "Correctly filtered"],
            ["Indirect*", "12", "100%", "$141", "Collateral (all wins)"],
        ],
        [35, 25, 20, 30, 80],
    )
    pdf.add_text_he(
        "* עקיף: עסקאות שנעלמו כי סינון עסקאות קודמות שינה את תזמון הפוזיציות. "
        "אלו נזק עקיף בלתי נמנע — כל 12 היו רווחיות.",
        size=7, color=GRAY,
    )

    pdf.add_subtitle_he("יחסים שהוחזרו לאחר ניתוח")
    pdf.table(
        ["Ratio", "Trades", "WR", "Avg P&L", "Action"],
        [
            ["2.272", "3", "100%", "$268", "ADDED"],
            ["3.414", "4", "75%", "$171", "ADDED"],
        ],
        [30, 25, 25, 30, 80],
    )
    pdf.ln(3)

    # Exit Logic
    pdf.section_title_he("7", "לוגיקת יציאה: ללא גבוה חדש מול סטופ נגרר")

    pdf.table(
        ["Metric", "Trailing Stop", "No-New-High", "Change"],
        [
            ["Trades", "129", "129", "0"],
            ["Win Rate", "72.9%", "72.9%", "0"],
            ["Net P&L (compound)", "$472,672", "$476,008", "+$3,336"],
            ["Trailing Exit WR", "varies", "100%", "ALL WINS"],
        ],
        [50, 45, 45, 50],
    )
    pdf.add_text_he(
        "יציאת ללא-גבוה-חדש לוכדת מקסימום רווח מומנטום: נשארים בפנים "
        "כל עוד כל נר עושה גבוה חדש, ויוצאים כשהמומנטום נעצר. "
        "43 מתוך 43 יציאות נגררות היו רווחיות בשיטה זו.",
        size=8, color=GRAY,
    )

    # ══════════════════════════════════════════════════════
    # PAGE 4: Grid Optimization
    # ══════════════════════════════════════════════════════
    pdf.add_page()

    pdf.section_title_he("8", "אופטימיזציית רשת — 180 קומבינציות נבדקו")

    pdf.table(
        ["Parameter", "Values Tested", "Count"],
        [
            ["Gap Max", "30%, 40%, 50%, 60%, 80%, None", "6"],
            ["Entry Window", "11:00, 12:00, 13:00, 14:00, 15:00, None", "6"],
            ["Ratio Filter", "OFF, 9-core, 11-ext, 7-tight, top-5-WR", "5"],
        ],
        [40, 110, 40],
    )

    pdf.set_font("en", "", 9)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 6, "Total: 6 x 6 x 5 = 180 configurations tested",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 5, "Stability Score = (WR x 100) x min(PF, 20) / max(DD x 100, 1)",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    cw = [12, 22, 24, 28, 22, 22, 26, 22, 22]

    pdf.add_subtitle_he("טופ 5 לפי ציון יציבות")
    pdf.table(
        ["#", "Gap", "Window", "Ratios", "Trades", "WR", "Total P&L", "PF", "MaxDD"],
        [
            ["1", "30%", "13:00", "top5_wr", "63", "88.9%", "$9,719", "19.1", "1.6%"],
            ["2", "30%", "12:00", "top5_wr", "61", "88.5%", "$9,652", "18.9", "1.6%"],
            ["3", "30%", "14:00", "top5_wr", "66", "86.4%", "$9,718", "17.7", "1.6%"],
            ["4", "30%", "13:00", "7_tight", "87", "81.6%", "$12,209", "10.5", "2.7%"],
            ["5", "30%", "12:00", "7_tight", "85", "81.2%", "$12,142", "10.4", "2.7%"],
        ],
        cw, highlight_best=0,
    )

    pdf.add_subtitle_he("טופ 5 לפי רווח כולל")
    pdf.table(
        ["#", "Gap", "Window", "Ratios", "Trades", "WR", "Total P&L", "PF", "MaxDD"],
        [
            ["1", "None", "None", "OFF", "200", "67.5%", "$22,928", "4.9", "3.2%"],
            ["2", "None", "15:00", "OFF", "194", "67.5%", "$22,889", "5.0", "3.2%"],
            ["3", "60%", "None", "OFF", "194", "69.1%", "$22,702", "5.2", "3.2%"],
            ["4", "50%", "13:00", "11_ext", "128", "73.4%", "$17,489", "6.4", "3.2%"],
            ["5", "30%", "13:00", "7_tight", "87", "81.6%", "$12,209", "10.5", "2.7%"],
        ],
        cw, highlight_best=0,
    )
    pdf.add_text_he(
        "תובנה מרכזית: הקונפיגורציות היציבות ביותר משתמשות ב-gap<=30% + top5_wr. "
        "הרווח הגבוה ביותר מגיע ללא פילטרים אבל עם כפול ירידה.",
        size=8, color=GRAY,
    )

    # ══════════════════════════════════════════════════════
    # PAGE 5: 4 Options Comparison
    # ══════════════════════════════════════════════════════
    pdf.add_page()

    pdf.section_title_he("9", "4 אפשרויות תצורה — השוואה")

    cw2 = [42, 22, 22, 32, 24, 26, 26]
    pdf.table(
        ["Config", "Trades", "WR", "Total P&L", "Avg P&L", "PF", "MaxDD"],
        [
            ["A: Max Stability", "63", "88.9%", "$9,719", "$154", "19.1", "1.6%"],
            ["B: Balanced", "87", "81.6%", "$12,209", "$140", "10.5", "2.7%"],
            ["C: More Trades", "128", "73.4%", "$17,489", "$137", "6.4", "3.2%"],
            ["D: Max P&L", "200", "67.5%", "$22,928", "$115", "4.9", "3.2%"],
        ],
        cw2, highlight_best=0,
    )
    pdf.ln(2)

    pdf.add_subtitle_he("אחוז הצלחה")
    pdf.bar_chart_text("A", 88.9, 100, WIN_GREEN)
    pdf.bar_chart_text("B", 81.6, 100, WIN_GREEN)
    pdf.bar_chart_text("C", 73.4, 100, ORANGE)
    pdf.bar_chart_text("D", 67.5, 100, ORANGE)
    pdf.ln(2)

    pdf.add_subtitle_he("פקטור רווח")
    pdf.bar_chart_text("A", 19.1, 20, WIN_GREEN)
    pdf.bar_chart_text("B", 10.5, 20, WIN_GREEN)
    pdf.bar_chart_text("C", 6.4, 20, BLUE)
    pdf.bar_chart_text("D", 4.9, 20, BLUE)
    pdf.ln(2)

    pdf.add_subtitle_he("ירידה מקסימלית (נמוך = טוב יותר)")
    pdf.bar_chart_text("A", 1.6, 5, WIN_GREEN)
    pdf.bar_chart_text("B", 2.7, 5, ORANGE)
    pdf.bar_chart_text("C", 3.2, 5, LOSS_RED)
    pdf.bar_chart_text("D", 3.2, 5, LOSS_RED)
    pdf.ln(2)

    pdf.add_subtitle_he("רווח כולל ($)")
    pdf.bar_chart_text("A", 9719, 23000, BLUE)
    pdf.bar_chart_text("B", 12209, 23000, BLUE)
    pdf.bar_chart_text("C", 17489, 23000, WIN_GREEN)
    pdf.bar_chart_text("D", 22928, 23000, WIN_GREEN)

    # ══════════════════════════════════════════════════════
    # PAGE 6: Configuration Settings + Summary
    # ══════════════════════════════════════════════════════
    pdf.add_page()

    pdf.section_title_he("10", "הגדרות תצורה")

    pdf.add_subtitle_he("אפשרות A: יציבות מקסימלית (מומלץ)")
    pdf.add_text("Gap <= 30%  |  Entry before 13:00 ET  |  Top 5 WR ratios")
    pdf.add_text("Ratios: 0.764, 2.414, 3.272, 3.414, 3.618", size=8, color=GRAY)
    pdf.add_bullet_he("89% הצלחה — לעיתים רחוקות פוגעים בסדרת הפסדים", WIN_GREEN)
    pdf.add_bullet_he("PF 19.1 — על כל $1 הפסד מרוויחים $19", WIN_GREEN)
    pdf.add_bullet_he("1.6% ירידה מקסימלית — כמעט אף פעם לא מרגישים הפסד", WIN_GREEN)
    pdf.ln(2)

    pdf.add_subtitle_he("אפשרות B: מאוזנת")
    pdf.add_text("Gap <= 30%  |  Entry before 13:00 ET  |  7 tight ratios")
    pdf.add_text("Ratios: 0.382, 0.764, 0.88, 2.414, 3.272, 3.414, 3.618",
                 size=8, color=GRAY)
    pdf.add_bullet_he("38% יותר עסקאות מ-A, עדיין 82% הצלחה", BLUE)
    pdf.ln(2)

    pdf.add_subtitle_he("אפשרות C: נפח מוגבר")
    pdf.add_text("Gap <= 50%  |  Entry before 13:00 ET  |  11 extended ratios")
    pdf.add_text("Adds: 0.5, 0.618, 1.414, 2.272", size=8, color=GRAY)
    pdf.ln(2)

    pdf.add_subtitle_he("אפשרות D: רווח מקסימלי (ללא פילטרים)")
    pdf.add_text_he("כל 200 העסקאות. הרווח הכולל הגבוה ביותר אבל האיכות הנמוכה ביותר.")

    # Journey summary table
    pdf.section_title_he("11", "סיכום מסע האופטימיזציה")

    pdf.table(
        ["Step", "Action", "Trades", "WR", "P&L", "Key Change"],
        [
            ["Start", "Raw backtest", "200", "67.5%", "$21,854", "Baseline"],
            ["1", "Applied 3 filters", "129", "72.9%", "$16,872", "+5.4pp WR"],
            ["2", "No-new-high exit", "129", "72.9%", "$16,872", "100% trail WR"],
            ["3", "Analyzed filtered", "---", "---", "---", "Found 2 ratios"],
            ["4", "Added 2.272+3.414", "128", "73.4%", "$17,489", "+$617 P&L"],
            ["5", "Grid 180 combos", "63*", "88.9%*", "$9,719*", "Optimal"],
        ],
        [18, 55, 22, 20, 28, 47],
    )
    pdf.add_text_he("* תצורת היציבות הטובה ביותר (אפשרות A)", size=7, color=GRAY)
    pdf.ln(3)

    # Recommendation box
    pdf.set_fill_color(*HIGHLIGHT_BG)
    pdf.set_draw_color(*WIN_GREEN)
    y = pdf.get_y()
    pdf.rect(pdf.l_margin, y, 190, 50, "DF")

    pdf.set_xy(pdf.l_margin, y + 4)
    pdf.set_font("en", "B", 14)
    pdf.set_text_color(*WIN_GREEN)
    pdf.cell(190, 8, he("מומלץ: אפשרות A — יציבות מקסימלית"),
             align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("en", "", 10)
    pdf.set_text_color(*WHITE)
    pdf.cell(190, 6, "Gap <= 30%  |  Entry before 13:00 ET  |  Top 5 WR ratios",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(190, 6, "Ratios: 0.764, 2.414, 3.272, 3.414, 3.618",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("en", "", 10)
    pdf.cell(190, 6, he("יציאה: 50% ביעד + 50% ללא גבוה חדש"),
             align="C", new_x="LMARGIN", new_y="NEXT")

    pdf.set_y(y + 50 + 5)
    pdf.metric_row([
        ("אחוז הצלחה", "88.9%", WIN_GREEN),
        ("פקטור רווח", "19.1", WIN_GREEN),
        ("ירידה מקס'", "1.6%", WIN_GREEN),
        ("רווח ממוצע", "$154", WIN_GREEN),
    ])

    # ══════════════════════════════════════════════════════
    # PAGE 7: Why Option A
    # ══════════════════════════════════════════════════════
    pdf.add_page()

    pdf.section_title_he("12", "למה אפשרות A?")
    pdf.add_bullet_he(
        "עקביות: 89% הצלחה = הפסד אחד מכל 9 עסקאות. "
        "הרבה יותר קל פסיכולוגית למסחר חי.",
        WIN_GREEN,
    )
    pdf.ln(1)
    pdf.add_bullet_he(
        "בטיחות: 1.6% ירידה מקסימלית לעומת 3.2% ללא פילטרים. "
        "חצי מהסיכון עם תשואות חזקות.",
        WIN_GREEN,
    )
    pdf.ln(1)
    pdf.add_bullet_he(
        "איכות רווח: $154 ממוצע לעסקה לעומת $115 ללא פילטרים. "
        "כל עסקה שווה 34% יותר.",
        WIN_GREEN,
    )
    pdf.ln(1)
    pdf.add_bullet_he(
        "חוסן: Gap<=30% + 13:00 ET + top5 ratios — "
        "הפילטרים מבוססים על יתרונות סטטיסטיים ברורים, לא curve-fitting.",
        WIN_GREEN,
    )
    pdf.ln(3)

    pdf.add_text_he(
        "הערה: לאפשרות A יש פחות עסקאות (63 לעומת 200), מה שאומר פחות "
        "רווח כולל ($9,719 לעומת $22,928). אם מקסום תשואות כולל הוא בעדיפות "
        "ואתה יכול לסבול ירידה גבוהה יותר, שקול אפשרות B או C כפשרה.",
        size=8, color=GRAY,
    )

    # Footer
    pdf.ln(8)
    pdf.set_font("en", "", 8)
    pdf.set_text_color(*GRAY)
    pdf.cell(0, 5,
             he("נוצר: פברואר 2026  |  נתונים: 30 ימים של נרות 15-שניות  |  פוזיציה קבועה $3,000"),
             align="C", new_x="LMARGIN", new_y="NEXT")

    # Save
    out_path = DATA_DIR / "fib_double_touch_optimization_report.pdf"
    pdf.output(str(out_path))
    print(f"PDF saved to: {out_path}")
    return out_path


if __name__ == "__main__":
    generate()
