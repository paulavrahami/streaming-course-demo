# מעבדת Kafka אמיתית — שלב אופציונלי

המעבדה הזו מחברת את אותו `StreamEngine` שמופיע במחברת לברוקר אמיתי. ה־producer וה־consumer הם תהליכי Python נפרדים; ההודעות עוברות דרך Kafka API, והמצב העסקי נשמר ב־SQLite. מסלול ההרצה המקומי והמחברת עובדים גם בלי Docker ובלי תלויות Kafka.

התרגילים כאן מיועדים ל־**PowerShell בתוך Cursor ב־Windows**, עם Python **3.13.12** שמנוהל ב־uv. פקודות Python תקפות גם ב־Linux וב־macOS; תחביר המשתנים בדוגמאות הוא של PowerShell.

## הרכיבים והגרסאות

| רכיב | גרסה במעבדה | תפקיד |
|---|---|---|
| Python | `3.13.12` | הפעלת הקוד והמחברת |
| לקוח Python | `confluent-kafka==2.16.1` | יצירת topics, שליחה, קריאה ו־commit ידני |
| Redpanda | `docker.redpanda.com/redpandadata/redpanda:v26.2.6` | ברוקר שתומך ב־Kafka API |
| Redpanda Console | `docker.redpanda.com/redpandadata/console:v3.14.0` | צפייה בהודעות ובקבוצות צרכנים |
| SQLite | כלול ב־Python | מצב מצטבר, מניעת ספירה כפולה, checkpoint, פלטים ו־DLQ |

ה־pins של Redpanda ו־Console תואמים לדוגמת single-broker הרשמית שנבדקה ב־8 בנובמבר 2026. ללקוח Confluent בגרסה שנבחרה יש wheel רשמי ל־**CPython 3.13 על Windows x86-64**; זה הבסיס לבחירה עבור סביבת המשתמש, ואינו מהווה טענה שהרצה על Windows בוצעה בזמן בניית המאגר. המקורות מופיעים בסוף המסמך.

`compose.yaml` מפרסם רק שני פורטים על המחשב המקומי:

- Kafka API: `127.0.0.1:19092`.
- Console: `127.0.0.1:18080`.

בתוך רשת Docker הכתובת היא `redpanda:9092`. לקוח Python שרץ על המחשב משתמש ב־`localhost:19092`. כתובת שפורסמה באמצעות `advertised.listeners` חייבת להיות נגישה ללקוח גם אחרי חיבור ה־bootstrap; לכן יש כאן כתובות נפרדות לפנים הרשת ולמחשב המארח.

## 1. התקנה והפעלת הברוקר

נדרשים uv, Docker Desktop עם **Linux containers** ו־Docker Compose v2. ב־Windows מקובל להפעיל את מנוע Docker Desktop באמצעות WSL 2. מדריך Redpanda הרשמי מציג את פקודות המעבדה שלו דרך WSL; במאגר הזה Docker מריץ את הברוקר בתוך container של Linux, ולקוח Python יכול לרוץ ב־PowerShell במחשב המארח.

פתחו terminal בשורש המאגר:

```powershell
uv sync --extra kafka
docker compose up --detach --wait --wait-timeout 120
docker compose ps
```

בדיקת הבריאות של הברוקר היא `rpk cluster health --exit-when-healthy`. Console תלוי בברוקר בריא. פתחו את [Redpanda Console](http://localhost:18080).

אם ההפעלה נכשלה, בדקו:

```powershell
docker compose logs --tail 80 redpanda console
```

הנתונים של הברוקר נשמרים ב־volume של פרויקט Compose. הפעלה מחדש אינה מוחקת אותם.

## 2. יצירת topics מפורשת

ניצור שמות ייחודיים לכל תרגול כדי שאפשר יהיה לחזור על ההוראות בלי לערבב הודעות מתרגול קודם. שמרו את המשתנים באותו terminal עד סוף התרגול:

```powershell
$lab = "lab-" + [guid]::NewGuid().ToString("N").Substring(0, 8)
$orders = "$lab-orders"
$reviews = "$lab-reviews"
$state = "runs/kafka/$lab"
$start = [DateTimeOffset]::UtcNow.ToString("o")
uv run --extra kafka python -m streaming_lab.kafka_demo topics --topics $orders $reviews
```

כל topic חדש נוצר עם partition יחיד, replication factor של 1 ו־`cleanup.policy=delete`. פקודת `topics` חוזרת בודקת את ה־topics הקיימים ואינה מוחקת אותם. Producer ו־consumer דורשים topic קיים; הם אינם יוצרים אותו בדרך אגב.

**Partition אחד הוא חלק מחוזה המעבדה.** בתוך partition קיים סדר לפי offsets. אין סדר כולל מובטח בין partitions או בין topics. המתאם דוחה topic עם יותר מ־partition אחד, משום שמנוע המעבדה שומר checkpoint יחיד. מעבר למספר partitions דורש checkpoints נפרדים, טיפול ב־rebalance וחלוקת מצב מתאימה.

מפתח הודעת order הוא `event_id`, ומפתח review הוא `product_id`. כך ניסיון חוזר של אותה הזמנה משתמש באותו מפתח, וביקורות של אותו מוצר נושאות מפתח משותף. עם partition יחיד כל ההודעות מגיעות לאותו partition; ההבדל בין המפתחות מציג את כוונת החלוקה לקראת הרחבה. מפתח Kafka אינו מסיר כפילויות עסקיות בעצמו.

## 3. Producer סופי ו־consumer סופי

שלחו 20 הזמנות לוגיות. אחרי כל הזמנה חמישית, ה־producer שולח שוב את אותם bytes ואת אותו מפתח:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo produce --topic $orders --kind order --count 20 --duplicate-every 5 --interval 0 --run-id "$lab-orders" --start-time $start
```

התוצאה הצפויה היא `logical_events=20`, ‏`physical_records=24`, ‏`acknowledged=24` ו־`failed=0`. `--count` של producer סופר אירועים לוגיים; עותקי ההדגמה מתווספים אליהם.

כעת צרכו את 24 הרשומות הפיזיות:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic $orders --group "$lab-revenue" --db "$state/orders.sqlite" --count 24 --timeout 30
```

פלט JSON כולל את מצב המנוע. צפו ל־20 הזמנות שהתקבלו, 4 כפילויות שנדחו מספירה עסקית ו־checkpoint של 23. הכנסה נשמרת במספר שלם של אגורות, באותה לוגיקה כמו במסלול המקומי.

`--count` של consumer סופר רשומות שנקראו מ־Kafka, לרבות כפילויות ו־replay. אם אין מספיק רשומות לפני המועד שהוגדר, התהליך נכשל בקוד יציאה שאינו אפס. הוא אינו ממתין בלי גבול כדי למלא את הספירה.

ה־producer משתמש ב־`enable.idempotence=true` וב־`acks=all`, אך עדיין שולח כפילויות מכוונות ברמת היישום. ה־delivery callbacks נבדקים בזמן `poll()` וגם אחרי `flush(timeout)`. כשל מסירה או הודעות שנותרו ללא אישור בסוף אינם מדווחים כהצלחה. תור הלקוח מוגבל, וגם המתנה לתור מלא מוגבלת בזמן.

## 4. הפעלה מחדש עם אותה קבוצה ואותו DB

הריצו שוב את אותו צרכן, הפעם לתצפית קצרה:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic $orders --group "$lab-revenue" --db "$state/orders.sqlite" --seconds 8 --timeout 30
```

אם לא נשלחו הודעות נוספות, `consumed` יהיה 0 והסכומים יישארו כפי שהיו. `--seconds` הוא מצב תצפית מוגבל בזמן: אפשר לסיים בהצלחה גם עם אפס הודעות, בתנאי שהצרכן הצליח לקבל partition. כאשר דרושה ספירה מסוימת, השתמשו ב־`--count`.

יש לשמור יחד **אותו cluster, כתובת bootstrap, topic, partition, group ו־DB**. המתאם שומר קישור מקור ב־SQLite ובודק אותו בחיבור הבא. גם כתובות שקולות, למשל `localhost:19092` ו־`127.0.0.1:19092`, נחשבות כאן לבחירות שונות; המשיכו עם אותה כתובת בדיוק.

אם ה־commit של הקבוצה ב־Kafka מתקדם יותר מה־checkpoint ב־SQLite, המתאם עוצר: פתיחת DB ריק עם קבוצה שכבר התקדמה הייתה גורמת לדילוג על השפעות עסקיות שכבר אינן ב־DB הזה. הפתרון הוא להחזיר את ה־DB התואם, או לבחור קבוצה חדשה **וגם DB חדש**.

## 5. כשל בין SQLite לבין commit ב־Kafka

לשלב זה נשתמש בקבוצה וב־DB נוספים, שמתחילים לקרוא את אותן הודעות מההתחלה:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic $orders --group "$lab-crash" --db "$state/crash.sqlite" --count 1 --crash-before-broker-commit
$LASTEXITCODE
```

קוד יציאה **86** הוא תוצאה צפויה של הזרקת הכשל. התהליך עוצר אחרי שכתיבת SQLite של offset 0 נשמרה, ולפני אישורו ב־Kafka. זו הזרקת כשל בנקודת החוזה; היא אינה בדיקה של הפסקת חשמל או `SIGKILL`.

עתה הפעילו שוב, עם **אותה קבוצה ואותו קובץ**, בלי דגל הכשל:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic $orders --group "$lab-crash" --db "$state/crash.sqlite" --count 24 --timeout 30
```

offset 0 נקרא שוב. המנוע מזהה שהוא כבר בוצע לפי ה־checkpoint, אינו מעדכן שוב סכומים, והצרכן מאשר אותו. צפו ל־`replayed=1` ולתוצאה עסקית זהה לצרכן הראשון: 20 הזמנות ו־4 כפילויות עסקיות.

### סדר הפעולות וההבטחה המדויקת

לכל הודעה:

1. `engine.ingest(..., offset=offset)` מבצע transaction ב־SQLite. ה־checkpoint, מניעת כפילויות, המצב והפלטים המקומיים נשמרים יחד.
2. רק אחרי הצלחת ה־transaction מתבצע `consumer.commit(..., asynchronous=False)`.
3. הערך שמאושר ב־Kafka הוא **`offset + 1`**, כלומר ההודעה הבאה לקריאה.

| נקודת הכשל | מה קרה ב־SQLite | מה צפוי בהפעלה הבאה |
|---|---|---|
| לפני השלמת transaction | השינוי אינו נשמר | ההודעה תעובד מחדש |
| אחרי SQLite ולפני Kafka commit | ההשפעה וה־checkpoint נשמרו | הודעה חוזרת מזוהה כ־replay ואינה משנה שוב את הסכומים |
| commit ב־Kafka נכשל | SQLite כבר מעודכן | התהליך נכשל; הפעלה עם אותו DB וקבוצה סובלת קריאה חוזרת |
| אחרי commit מוצלח | SQLite ו־Kafka התקדמו | הקריאה ממשיכה מה־offset הבא |

זהו **עיבוד עם אפשרות למסירה חוזרת והשפעה מקומית אידמפוטנטית**, תחת חוזה המעבדה. אין transaction משותף ל־Kafka ול־SQLite, ואין כאן הבטחת exactly-once כללית ל־sink חיצוני. Kafka transactions אינם הופכים כתיבה ל־DuckDB, ל־SQLite, ל־API חיצוני או למסד וקטורים לחלק מאותו transaction באופן אוטומטי.

גם קריאה של JSON פגום, bytes שאינם UTF-8, ערך שאינו object או tombstone אינה גורמת ללולאת poison message: המתאם מעביר רשומה לא תקינה למנוע, המנוע שומר אותה ב־DLQ המקומי עם ה־checkpoint, ורק לאחר מכן מתבצע commit ב־Kafka.

## 6. שתי קבוצות צרכנים עצמאיות

קבוצה חדשה יכולה לקרוא שוב את אותו topic לצורך תוצר אחר:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic $orders --group "$lab-audit" --db "$state/audit.sqlite" --count 24 --timeout 30
```

לקבוצה הזו יש offsets משלה ו־DB משלה. היא אמורה להגיע לאותם סכומים. אין לשתף את אותו DB בין קבוצות עצמאיות. המעבדה גם אינה מיועדת לשני תהליכי צריכה בו־זמנית באותה קבוצה: עם partition יחיד רק אחד יקבל עבודה, וה־DB המקומי אינו מחסן מצב מבוזר שמועבר אוטומטית בין חברי קבוצה.

## 7. ביקורות שליליות והתראה עמידה

שלחו תשע ביקורות שליליות של אותו מוצר, עם שלושה עותקים מכוונים:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo produce --topic $reviews --kind review --count 9 --burst --duplicate-every 3 --interval 0 --run-id "$lab-reviews" --start-time $start
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic $reviews --group "$lab-alerts" --db "$state/reviews.sqlite" --count 12 --timeout 30
```

צפו לתשע ביקורות שהתקבלו, שלוש כפילויות עסקיות ו־**התראה מקומית אחת** עבור המוצר והחלון. ההתראה נוצרת כשהמונה מגיע לסף של לפחות שלוש ביקורות שליליות, ונשמרת באופן שמונע יצירה חוזרת של אותה התראה. אין במעבדה שליחה אוטומטית של email, Slack או webhook; הוספת יעד כזה תדרוש חוזה מסירה נוסף, למשל outbox עם idempotency key.

## 8. מדוע חלון עדיין פתוח?

הצרכן משתמש בהגדרות ברירת המחדל של המנוע: חלונות event time של 60 שניות ו־grace של 10 שניות. ה־watermark מתקדם כשמגיע אירוע עם event time מתקדם. המתנה של דקה בשעון המחשב אינה מקדמת אותו כשאין אירועים חדשים.

לכן הרצת 20 הזמנות בבת אחת עשויה להראות סכומים ב־`totals` וב־`open_windows`, ועדיין לא להראות חלונות סגורים ב־`revenue_windows`. סיום producer סופי או Ctrl+C אינם הוכחה שהזרם בעולם הסתיים, ולכן מתאם Kafka אינו קורא ל־`finalize_demo()`.

אחרי השלמת תרגילי ה־replay, אפשר להמחיש סגירה באמצעות אירוע עם event time מתקדם:

```powershell
$later = [DateTimeOffset]::Parse($start).AddMinutes(2).ToString("o")
uv run --extra kafka python -m streaming_lab.kafka_demo produce --topic $orders --kind order --count 1 --interval 0 --run-id "$lab-later" --start-time $later
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic $orders --group "$lab-revenue" --db "$state/orders.sqlite" --count 1 --timeout 30
```

כעת אירוע נוסף מקדם את ה־watermark וסוגר חלון ישן כשמועד הסגירה שלו חלף. ההזמנה החדשה עצמה שייכת לחלון חדש. זו הדגמה מבוקרת של event time; במערכת אמיתית צריך להגדיר גם מדיניות לשעונים שגויים ולאירועים עתידיים. המסלול המקומי במחברת מציג את אותה התקדמות עם זמנים מואצים.

## 9. זרם רציף בשני terminals

לתרגול ממושך אפשר לבחור מצב רציף במפורש. השתמשו ב־topic חדש או המשיכו עם קבוצה וה־DB התואם שלה. בדוגמה הבאה הכתובות הן ברירות המחדל; החליפו את `YOUR_TOPIC`, ‏`YOUR_GROUP` והנתיב בערכים שבחרתם.

Terminal ראשון:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo consume --topic YOUR_TOPIC --group YOUR_GROUP --db runs/kafka/continuous.sqlite --continuous
```

Terminal שני:

```powershell
uv run --extra kafka python -m streaming_lab.kafka_demo produce --topic YOUR_TOPIC --kind order --continuous --interval 0.2 --duplicate-every 10
```

עוצרים באמצעות Ctrl+C. ה־producer מנסה להשלים מסירות בגבול הזמן ומדווח כשל אם נותרו מסירות לא מאושרות. ה־consumer סוגר חיבור בלי auto commit. מצב רציף אינו ברירת המחדל, ואינו משמש בבדיקת ה־CI.

### זרם רציף, טיפול יחיד ו־micro-batch

זרם רציף מתאר מקור שממשיך ליצור אירועים עד שעוצרים אותו. אפשר לעבד זרם כזה אירוע אחר אירוע, או לאסוף קבוצה קטנה עד שמגיעים למספר אירועים או לפרק זמן מוגדר. הבחירה בקבוצה קטנה היא micro-batch: היא עשויה להפחית את מספר הפעולות מול בסיס הנתונים, במחיר המתנה עד שהקבוצה מוכנה.

במתאם Kafka כאן כל רשומה נמסרת ל־`StreamEngine.ingest` בנפרד, עם טרנזקציית SQLite ו־commit לברוקר אחריה. הלקוח יכול לקבץ הודעות למשלוח ברשת; קיבוץ ברמת התעבורה אינו משנה את חוזה העיבוד העסקי. מעבר ל־micro-batch בצרכן מחייב לכתוב יחד את השפעות הקבוצה ואת ה־checkpoint המקומי, ואז לעשות commit לברוקר של ה־offset האחרון בקבוצה ועוד אחד. אין לאשר offsets של אירועים שטרם נשמרו. המחברת מדגימה את השוואת אופני העיבוד במסלול המקומי; מתאם Kafka הזה נשאר במסלול של רשומה אחת בכל טרנזקציה.

## 10. בדיקות

### בדיקות חוזה ללא broker

```powershell
uv run python -m unittest discover -s tests -p test_kafka_contract.py -v
```

הבדיקות משתמשות ב־SQLite אמיתי ובתחליפים קטנים לממשק Kafka. הן בודקות את סדר הפעולות, `offset + 1`, התאוששות אחרי כשל, שגיאות commit, DLQ, קישור DB למקור, בדיקת פערי offsets, מפתחות ודטרמיניזם, תור מלא, callback שנכשל בזמן flush וגבולות זיכרון של דיווח שגיאות. הן אינן מהוות בדיקת פרוטוקול או תאימות Redpanda.

### בדיקת אינטגרציה מול broker אמיתי

```powershell
uv run --extra kafka python scripts/kafka_smoke.py
```

ללא פרמטרים הפקודה מפעילה פרויקט Compose נפרד עם שם ופורטים ייחודיים, יוצרת topics וקבוצות ייחודיים, מפעילה producers ו־consumers בתהליכים נפרדים, ולבסוף מורידה רק את ה־containers וה־volume של אותו פרויקט בדיקה. היא אינה מוחקת את פרויקט המעבדה הרגיל.

לבדיקה נגד broker מקומי שכבר פועל:

```powershell
uv run --extra kafka python scripts/kafka_smoke.py --bootstrap localhost:19092
```

במצב הזה הבדיקה מוחקת בסיום **רק את ה־topics והקבוצות הייחודיים שהיא יצרה**. אין reset לקבוצה קיימת ואין מחיקה של DB של המשתמש. השתמשו בברוקר מעבדה שבו מותר ליצור ולמחוק את משאבי הבדיקה. שגיאת ניקוי משאבים גורמת לתוצאת כשל, ולא מוסתרת כהצלחה.

הבדיקה בוחנת יצירה מפורשת חוזרת של topics, אישורי מסירה, 20 הזמנות לוגיות מתוך 24 רשומות, הפעלה מחדש ללא עבודה נוספת, כשל אחרי SQLite ולפני commit, קבוצה עצמאית עם DB משלה, דחיית DB ריק עם קבוצה שכבר התקדמה, timeout כשאין מספיק הודעות, וביקורות שליליות עם התראה יחידה.

### מה אומת בזמן בניית המאגר

מצב אימות נכון ל־8 בנובמבר 2026:

- 21 בדיקות חוזה עברו על CPython 3.13.12 ב־Linux.
- כל תשעת תרחישי האינטגרציה עברו מול Apache Kafka 4.4.0 אמיתי ב־Linux עם Java 21 ו־CPython 3.13.12: יצירת topics, מסירה, commit, restart, התאוששות מכשל, קבוצות עצמאיות, דחיית DB שאינו תואם, timeout ו־alert ללא כפילות. בתוצאת ההזמנות התקבלו 20 הזמנות ו־367,630 אגורות מתוך 24 הודעות; בתוצאת הביקורות התקבלו 9 ביקורות שליליות ו־alert אחד מתוך 12 הודעות. המשאבים הזמניים נוקו והברוקר נעצר בסיום.
- Docker Desktop, תצורת Redpanda ב־Compose והרצה ב־Windows לא הורצו בסביבת הבנייה. סקריפט האינטגרציה מוכן להרצת Compose ב־Linux CI; תוצאה מוצלחת מול Apache Kafka אינה תחליף להרצת התצורה הזו עצמה.
- Quix Streams לא הותקן ולא הורץ כחלק מהמאגר הזה.

## 11. מגבלות שתורמות להבנת החוזה

המעבדה משתמשת בברוקר יחיד וברפליקה יחידה לצורכי לימוד. היא אינה בוחנת שרידות של cluster. Producer idempotence מטפלת בניסיונות חוזרים של הפרוטוקול בתוך חוזה הלקוח; אותה הזמנה שנשלחת שוב ברמת היישום עדיין דורשת `event_id` יציב ומניעת ספירה כפולה במצב העסקי.

ה־topics בדוגמה אינם compacted, ואין כאן כתיבה ב־Kafka transactions. המנוע דורש offsets רציפים מ־0. ביישום כללי, log compaction, רשומות בקרה של transactions או מחיקת נתונים עקב retention יכולים ליצור חורים או נקודת התחלה גבוהה יותר. המתאם עוצר בפער במקום לנחש מה דולג. לפני הצריכה נבדקים ה־low/high watermarks, ה־commit של הקבוצה וה־checkpoint המקומי; הבדיקה אינה תחליף לגיבויים ולניהול זהות topic לאורך מחיקה ויצירה מחדש.

מניעת כפילויות נשמרת ב־SQLite לאורך חיי ה־DB. מנגנון זה מציג נכונות והתאוששות; הוא אינו כולל מדיניות TTL או צמצום טבלאות למערכת שרצה חודשים. גם החלפת שרת, העברת מצב, הרשאות, TLS, schema registry, אימות סכמות מרוכז וניטור תפעולי אינם נכללים בתרגול הבסיסי.

## 12. הקשר לקוד Quix Streams המקורי

המקור של הקורס משתמש ב־Quix Streams. לצורך המעבדה העצמאית נבחר לקוח Confluent ישיר כדי לחשוף את גבול האחריות בין delivery, ‏commit ומצב SQLite ולהשתמש במנוע אחד בשני מסלולי ההרצה.

אין להסיק מכך ש־Quix אינו תומך ב־Windows או ב־Python מודרני. בזמן הבדיקה, PyPI מציג `quixstreams==3.28.1`, דרישת Python של `>=3.11,<4` וסיווג Windows. התיעוד העדכני מציין גם יכולות exactly-once המבוססות על Kafka transactions. גרסה זו לא הורצה כאן, ואין במאגר notebook שמציג הרצת Quix כאילו בוצעה.

| חלק במקור | הסיכון שנמצא | מה מדגים המאגר העצמאי |
|---|---|---|
| producer רציף ונתוני reviews מנתיב חיצוני | תלות במבנה קורס אחר וקושי לסיים הרצה צפויה | מחולל עצמאי, ספירה סופית, מצב רציף מפורש ואישורי מסירה נבדקים |
| `revenue_app.py` וכתיבה ל־DuckDB בתוך callback | Kafka/state ו־DuckDB אינם transaction אחד; אין להסיק exactly-once ל־sink חיצוני | transaction מקומי של מצב, dedupe, checkpoint ופלט; אחריו Kafka commit, עם תרגיל replay |
| timestamps ללא timezone וסכומים ב־float | פרשנות זמן לא חד־משמעית ושגיאות ייצוג כספי | event time עם timezone וסכומים שלמים באגורות |
| התראה מודפסת כאשר `count == 3` | הדפסה אינה פלט עמיד; restart ומסירה חוזרת דורשים חוזה נוסף | סף `>=3` וייחודיות התראה לפי מוצר וחלון, נשמרים ב־SQLite |
| `reviews_to_vectors.py` | ייבוא מתרגיל RAG אחר וכתיבת vectors שעלולה לחזור ב־replay | RAG הוא תרגיל הרחבה נפרד; אין כאן טענה לאינדוקס וקטורי או להרצת RAG |

הרחבת Quix עתידית יכולה להשתמש באותה סכמת אירועים ובאותם תרחישי כשל, תוך בחירה מפורשת של store, ‏processing guarantee ו־sink. הרחבת RAG צריכה להגדיר embeddings, דגם וממד קבועים, upsert לפי `event_id`, התאוששות וסט בדיקה לאיכות האחזור. חיפוש טקסט מקומי הוא כלי לבדיקת הנתונים, ולא מערכת RAG.

## 13. תקלות נפוצות וכיבוי

| תסמין | בדיקה או פעולה |
|---|---|
| `Topic ... unavailable` | הריצו `topics --topics ...` ובדקו שהשם וה־bootstrap זהים בכל הפקודות |
| `Connection refused` או כשל metadata | ודאו ש־Docker פועל, שהברוקר healthy ושפורט 19092 אינו תפוס; בדקו גם את הכתובת המפורסמת ללקוח |
| consumer מסיים ב־timeout | בדקו כמה רשומות פיזיות נשלחו; `20` עם `--duplicate-every 5` יוצר `24`; ודאו שאין צרכן אחר באותה קבוצה |
| commit של הקבוצה מתקדם מ־SQLite | החזירו את ה־DB התואם או בחרו קבוצה חדשה עם DB חדש |
| DB מקושר למקור אחר | שמרו את השילוב המקורי או השתמשו בנתיב חדש; אל תמחזרו DB מקומי לתרגיל Kafka |
| offset דרוש נמחק עקב retention | replay מלא כבר אינו אפשרי מה־log הזה; המעבדה אינה מאפסת offsets בשקט |
| חלון הכנסות אינו מופיע כפלט סופי | בדקו event time ו־watermark; ייתכן שהחלון עדיין פתוח באופן תקין |
| Windows משתמש ב־Python אחר | בדקו `uv run python --version` ובחרו ב־Cursor את interpreter של סביבת המאגר |

לכיבוי בלי מחיקת ה־volume:

```powershell
docker compose down
```

לאיפוס יזום של **כל נתוני הברוקר של פרויקט המעבדה הזה**, אפשר להשתמש ב־`docker compose down --volumes` רק כשאכן רוצים למחוק את ה־topics וה־offsets שלו. לאחר איפוס כזה התחילו תרגול עם שמות topics, קבוצות ו־DB חדשים. פקודות ההדגמה הרגילות אינן מבצעות איפוס כזה.

## מקורות רשמיים

נבדקו ב־8 בנובמבר 2026. גרסאות עתידיות של עמודי התיעוד עשויות להשתנות; ה־pins שבמאגר מגדירים את המעבדה הזו.

- [Confluent Kafka Python 2.16.1 — קובצי ההפצה, כולל CPython 3.13 Windows x86-64](https://pypi.org/project/confluent-kafka/2.16.1/).
- [Confluent Python API — producer callbacks, ‏flush, ‏commit ו־TopicPartition](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html).
- [Confluent Python client overview — producer, consumer ו־AdminClient](https://docs.confluent.io/kafka-clients/python/current/overview.html).
- [Redpanda — single broker ו־Console ב־Docker Compose](https://docs.redpanda.com/labs/docker-compose/single-broker/).
- [Redpanda — בדיקת בריאות באמצעות rpk](https://docs.redpanda.com/streaming/current/reference/rpk/rpk-cluster/rpk-cluster-health/).
- [Docker Desktop — backend מבוסס WSL 2](https://docs.docker.com/desktop/features/wsl/).
- [Quix Streams 3.28.1 — דרישות Python, סיווג פלטפורמות וקישורי התיעוד](https://pypi.org/project/quixstreams/3.28.1/).
- [Apache Kafka 4.4 quickstart — Java, ‏KRaft והפעלה מקומית](https://kafka.apache.org/44/getting-started/quickstart/).
- [Apache Kafka — הורדות רשמיות](https://kafka.apache.org/downloads/).
