# הרצה ב-Windows עם Cursor ו-uv

<div dir="rtl">

## 1. פותחים עותק של הפרויקט

אפשר לשכפל את המאגר באמצעות Git או לחלץ את קובץ הפרויקט. פתחו ב-Cursor את התיקייה **streaming-course-demo** עצמה. המחברת כבר נמצאת בשורש הפרויקט; אין צורך להעתיק אותה לתיקייה אחרת.

אם מורידים ZIP, יש לחלץ אותו לפני פתיחת המחברת. יש לוודא שבאותה תיקייה נמצאים pyproject.toml, uv.lock, src והמחברת.

## 2. מתקינים ומסנכרנים

בטרמינל PowerShell בתוך Cursor בדקו uv --version. אם uv מותקן, המשיכו ישירות. אם אינו מותקן, השתמשו בהוראות [ההתקנה הרשמיות](https://docs.astral.sh/uv/getting-started/installation/), ואז פתחו טרמינל חדש.

</div>

```powershell
uv python install 3.13.12
uv sync --locked
uv run python --version
uv run python -c "import streaming_lab; print('Streaming package is ready')"
```

<div dir="rtl">

הפרויקט יוצר לעצמו סביבת .venv. הפקודות של uv משתמשות בה ישירות, ולכן אין צורך להריץ Activate.ps1 או לשנות ExecutionPolicy כדי להפעיל אותה.

## 3. בוחרים את המפרש של המחברת

פתחו Streaming_Lab_HE.ipynb. בחרו Select Kernel, ואז את Python של .venv. ב-Windows הנתיב הוא .venv\Scripts\python.exe. ההרחבות Python ו-Jupyter מאפשרות עבודה עם מחברות בעורך.

אם הסביבה אינה מוצגת, רשמו kernel:

</div>

```powershell
uv run python -m ipykernel install --user --name streaming-course-demo --display-name "Python 3.13.12 (Streaming)"
```

<div dir="rtl">

לאחר מכן בחרו Python 3.13.12 (Streaming). בתא הראשון מוצגים גם הגרסה וגם sys.executable. שניהם צריכים להתאים לסביבה הזאת.

## 4. מריצים ובודקים

בהרצה הראשונה אפשר ללחוץ Run All. בשיעור כדאי להריץ תא אחד בכל פעם: לקרוא את ההסבר, לנבא את התוצאה ולהשוות לטבלה. ההרצה המקומית כוללת כמה הדגמות קצרות שמייצרות הזמנות לאורך זמן.

אם מתקבלת שגיאה אחרי דילוג על תאים, הפעילו Restart Kernel ואז Run All. משתנים במחברת תלויים בתאים הקודמים. ההרצה החוזרת יוצרת תיקייה חדשה ושומרת את תוצאות ההרצה הקודמת.

## 5. פותחים את התוצרים

בתא היצוא מופיע הנתיב המדויק של תיקיית הריצה תחת lab_runs. פתחו אותה בסייר הקבצים של Cursor. פתחו report.html בדפדפן. קובצי CSV משתמשים ב-UTF-8 עם סימון קידוד, כדי להקל על פתיחתם ב-Excel בעברית.

המחברת עצמה קוראת גם נתונים ישירות מתוך SQLite; אין צורך להתקין כלי SQL נוסף כדי לבדוק את הסכומים.

## עדכון העיצוב והתצוגות

הטבלאות במחברת ודוח ה-HTML משתמשים ברקע כחול כהה, כותרות בטורקיז ושורות בגוונים מתחלפים. החלטות העיבוד מופיעות גם בתגיות צבע: accepted בירוק, duplicate בזהב ו-late בוורוד. בטבלאות רחבות אפשר לגלול אופקית בתוך הטבלה.

כדי לעדכן עותק ששוכפל מ-GitHub, הריצו מתוך תיקיית הפרויקט:

</div>

```powershell
git pull --ff-only
uv sync --locked
```

<div dir="rtl">

במחברת לחצו Restart Kernel ואז Run All. הפעלה מחדש של ה-Kernel טוענת את קוד התצוגה המעודכן, והרצת התאים מחליפה את הפלט הישן. פתחו את report.html מתיקיית הריצה החדשה; דוחות שנוצרו לפני העדכון שומרים על העיצוב שבו נוצרו.

## חלופה: מחברת בדפדפן

אם תמיכת המחברות ב-Cursor אינה זמינה, אפשר להפעיל JupyterLab באותה סביבת uv:

</div>

```powershell
uv run --with jupyterlab jupyter lab Streaming_Lab_HE.ipynb
```

<div dir="rtl">

הפקודה עשויה להוריד JupyterLab בהרצה הראשונה. פתחו את הקישור המקומי שמופיע בטרמינל והשאירו את הטרמינל פתוח. אין צורך ב-Google Colab או בחשבון Google.

## תקלות נפוצות

| הודעה או מצב | מה לבדוק |
|---|---|
| uv אינו מזוהה | התקנה רשמית ופתיחת טרמינל חדש |
| ModuleNotFoundError: streaming_lab | הריצו uv sync --locked בשורש הפרויקט ובחרו .venv במחברת |
| גרסת Python אינה 3.13.12 | בדקו את Kernel המחברת; גרסת הטרמינל לבדה אינה מספיקה |
| NameError אחרי הרצת תא | הפעילו את התאים הקודמים או Restart Kernel ו-Run All |
| אין עדיין חלונות סגורים | בדקו חלונות פתוחים והתקדמות זמן האירוע; שתיקה אינה סוגרת חלון |
| port is already allocated במסלול הברוקר | בדקו שירותים המשתמשים בפורט; ראו KAFKA_LAB.md |
| Docker אינו פעיל | הריצו את המסלול המקומי, או הפעילו Docker Desktop לפני מסלול הרשות |

</div>
