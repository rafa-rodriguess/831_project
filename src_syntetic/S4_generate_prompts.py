"""
S4_generate_prompts.py
──────────────────────
Generates a natural-language prompt for each synthetic student persona.
Each prompt embeds the persona profile and all 24 interview questions from
Appendix A of proposal.md (6 domains, Q1–Q24), instructing Claude to answer
in first person as that student and return a fixed JSON object.

Strategy: one-shot (all 24 questions in a single API call per persona)
  - Claude's 200K-token context window has no difficulty fitting all questions
  - Persona stays coherent across all 24 answers in one session
  - 1,300 API calls total instead of 31,200

Name generation:
  - Each persona receives a unique full name matching their gender
  - Names drawn from ~25 nationalities (Brazilian, American, British, French,
    German, Spanish, Mexican, Italian, Japanese, Chinese, Korean, Indian,
    Nigerian, Ghanaian, Arabic, Turkish, Russian, Polish, Swedish, Dutch,
    Vietnamese, Filipino, Ethiopian, Argentine, Iranian)
  - Assigned deterministically by row index (seeded), guaranteed unique

Inputs
──────
outputs/data/synthetic/student_personas.csv   (1300 × 38)

Outputs
───────
outputs/data/synthetic/student_personas_with_prompts.csv   (1300 rows, +2 cols)
outputs/metadata/s4_prompt_audit.json
"""

import json
import os
import random

import numpy as np
import pandas as pd

# ── name pools (diverse international names, paired by cultural group) ────────
# Each tuple: (female_first, male_first, shared_last)
# ~25 cultural groups × ~12 entries each → >300 unique full names per gender

_NAME_GROUPS = [
    # (female_firsts, male_firsts, last_names)

    # Brazilian
    (["Ana", "Beatriz", "Camila", "Daniela", "Fernanda", "Gabriela", "Helena",
      "Isabela", "Juliana", "Larissa", "Mariana", "Natália"],
     ["Lucas", "Gabriel", "Mateus", "Rafael", "Pedro", "Gustavo", "Felipe",
      "Henrique", "Bruno", "Thiago", "Eduardo", "Leonardo"],
     ["Souza", "Oliveira", "Santos", "Ferreira", "Costa", "Alves", "Rodrigues",
      "Martins", "Carvalho", "Araújo", "Melo", "Lima"]),

    # American / Canadian (Anglo)
    (["Emma", "Olivia", "Sophia", "Mia", "Charlotte", "Amelia", "Harper",
      "Evelyn", "Abigail", "Emily", "Madison", "Chloe"],
     ["Liam", "Noah", "Oliver", "Elijah", "James", "Aiden", "Mason",
      "Ethan", "Logan", "Jackson", "Sebastian", "Carter"],
     ["Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
      "Wilson", "Anderson", "Taylor", "Moore", "Martin"]),

    # British
    (["Alice", "Grace", "Florence", "Harriet", "Poppy", "Scarlett", "Daisy",
      "Freya", "Isla", "Rosie", "Imogen", "Ellie"],
     ["Jack", "Harry", "Charlie", "George", "Alfie", "Freddie", "Archie",
      "Leo", "Oscar", "Henry", "Edward", "Arthur"],
     ["Smith", "Jones", "Taylor", "Davies", "Evans", "Thomas", "Roberts",
      "Hughes", "Walker", "White", "Hall", "Green"]),

    # French
    (["Camille", "Chloé", "Inès", "Léa", "Lucie", "Manon", "Marie",
      "Noémie", "Sarah", "Sophie", "Juliette", "Élise"],
     ["Louis", "Théo", "Hugo", "Tom", "Mathis", "Noah", "Baptiste",
      "Antoine", "Clément", "Nathan", "Alexandre", "Raphaël"],
     ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard",
      "Petit", "Durand", "Leroy", "Moreau", "Simon", "Laurent"]),

    # German / Austrian
    (["Anna", "Clara", "Elena", "Hannah", "Katharina", "Laura", "Lisa",
      "Lena", "Maja", "Nina", "Luisa", "Johanna"],
     ["Leon", "Lukas", "Finn", "Jonas", "Ben", "Elias", "Felix",
      "Maximilian", "Jan", "Paul", "David", "Tobias"],
     ["Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer",
      "Wagner", "Becker", "Schulz", "Hoffmann", "Koch", "Richter"]),

    # Spanish (Spain)
    (["Valentina", "Sofía", "Martina", "Lucía", "Paula", "Carmen",
      "Marta", "Alba", "Nora", "Irene", "Marina", "Claudia"],
     ["Alejandro", "Daniel", "Diego", "Carlos", "Pablo", "Miguel",
      "Javier", "Adrián", "Sergio", "Álvaro", "Marcos", "Jorge"],
     ["García", "Martínez", "López", "Sánchez", "González", "Pérez",
      "Rodríguez", "Fernández", "Torres", "Ramírez", "Moreno", "Navarro"]),

    # Mexican / Central American
    (["Ximena", "Valeria", "Renata", "Paola", "Andrea", "Alejandra",
      "Miriam", "Guadalupe", "Itzel", "Verónica", "Fabiola", "Mónica"],
     ["Óscar", "Emilio", "Ricardo", "Antonio", "Marco", "Rodrigo",
      "Ernesto", "Cristóbal", "Arturo", "Ignacio", "Fernando", "Roberto"],
     ["Hernández", "Reyes", "Cruz", "Flores", "Guerrero", "Vargas",
      "Morales", "Castillo", "Vázquez", "Salinas", "Aguilar", "Mendez"]),

    # Italian
    (["Giulia", "Francesca", "Chiara", "Eleonora", "Federica", "Ginevra",
      "Ilaria", "Matilde", "Rossana", "Silvia", "Valentina", "Alessia"],
     ["Marco", "Luca", "Matteo", "Lorenzo", "Riccardo", "Gabriele",
      "Alessandro", "Francesco", "Stefano", "Paolo", "Davide", "Andrea"],
     ["Rossi", "Ferrari", "Russo", "Esposito", "Bianchi", "Romano",
      "Colombo", "Ricci", "Marino", "Greco", "Bruno", "Gallo"]),

    # Japanese
    (["Yuki", "Sakura", "Hana", "Akiko", "Yumi", "Keiko", "Nana",
      "Rin", "Saki", "Yuna", "Aoi", "Miku"],
     ["Hiroshi", "Kenji", "Takeshi", "Yuto", "Haruto", "Sora", "Ren",
      "Daiki", "Shota", "Riku", "Taiga", "Ryota"],
     ["Tanaka", "Yamamoto", "Suzuki", "Watanabe", "Ito", "Kobayashi",
      "Nakamura", "Fujita", "Hayashi", "Kimura", "Saito", "Kato"]),

    # Chinese (Mainland)
    (["Wei", "Mei", "Lin", "Fang", "Xin", "Jing", "Ying",
      "Hong", "Yan", "Qing", "Hua", "Rong"],
     ["Wei", "Lei", "Fang", "Ming", "Jian", "Hao", "Long",
      "Tao", "Bin", "Peng", "Chao", "Yang"],
     ["Zhang", "Wang", "Li", "Liu", "Chen", "Yang", "Huang",
      "Zhao", "Wu", "Zhou", "Xu", "Sun"]),

    # Korean
    (["Ji-yeon", "Soo-jin", "Minji", "Yuna", "Jiyoung", "Eunji",
      "Hyuna", "Sooyeon", "Dahye", "Jisoo", "Nayeon", "Chaeyoung"],
     ["Min-jun", "Seo-jun", "Hyun-woo", "Ji-ho", "Dong-hyun", "Tae-yang",
      "Jae-won", "Sung-min", "Kang-min", "Joon-ho", "Woo-jin", "Byung-ho"],
     ["Kim", "Lee", "Park", "Choi", "Jung", "Kang", "Cho",
      "Yoon", "Jang", "Lim", "Han", "Oh"]),

    # Indian (Hindi / South Asian)
    (["Priya", "Anjali", "Kavya", "Pooja", "Shreya", "Ananya",
      "Divya", "Riya", "Nisha", "Meera", "Sonal", "Deepa"],
     ["Arjun", "Vikram", "Rahul", "Amit", "Suresh", "Rajesh",
      "Ankit", "Rohan", "Karan", "Nikhil", "Vivek", "Siddharth"],
     ["Sharma", "Patel", "Singh", "Kumar", "Gupta", "Agarwal",
      "Nair", "Reddy", "Rao", "Verma", "Joshi", "Mehta"]),

    # Nigerian / West African
    (["Amara", "Chidinma", "Adaeze", "Ngozi", "Ifunanya", "Chinyere",
      "Aisha", "Halima", "Zainab", "Fatimah", "Bola", "Temi"],
     ["Chukwuemeka", "Adewale", "Babatunde", "Oluwaseun", "Emeka",
      "Tunde", "Seun", "Femi", "Kunle", "Sola", "Dotun", "Gbemi"],
     ["Okafor", "Eze", "Obi", "Adeyemi", "Nwosu", "Chukwu",
      "Okeke", "Adeola", "Afolabi", "Balogun", "Adeleke", "Adebayo"]),

    # Ghanaian / East African
    (["Abena", "Akosua", "Ama", "Efua", "Adwoa", "Afia",
      "Selam", "Meron", "Hiwot", "Birhan", "Rahel", "Tigist"],
     ["Kwame", "Kofi", "Kweku", "Yaw", "Kojo", "Fiifi",
      "Dawit", "Yonas", "Bereket", "Haile", "Tesfaye", "Girma"],
     ["Mensah", "Asante", "Boateng", "Owusu", "Acheampong",
      "Tekeste", "Hailu", "Wolde", "Tadesse", "Bekele", "Negash", "Gebru"]),

    # Arabic / Middle Eastern
    (["Fatima", "Aisha", "Layla", "Nour", "Sara", "Rania",
      "Dina", "Rana", "Lina", "Hana", "Tamar", "Yasmin"],
     ["Omar", "Ahmed", "Mohammed", "Ibrahim", "Khalid", "Yusuf",
      "Hassan", "Ali", "Tariq", "Karim", "Nasser", "Samir"],
     ["Al-Rashid", "Hassan", "Ibrahim", "Khalil", "Mansour",
      "Nasser", "Omar", "Saleh", "Yousef", "Fahd", "Jaber", "Sudairi"]),

    # Turkish
    (["Ayşe", "Fatma", "Zeynep", "Elif", "Emine", "Hatice",
      "Meryem", "Deniz", "Selin", "Esra", "Büşra", "Gizem"],
     ["Mehmet", "Ali", "Mustafa", "Ahmet", "Hüseyin", "Hasan",
      "İbrahim", "Ömer", "Yasin", "Murat", "Emre", "Serkan"],
     ["Yilmaz", "Kaya", "Demir", "Şahin", "Çelik", "Yıldız",
      "Öztürk", "Aydin", "Doğan", "Arslan", "Koç", "Polat"]),

    # Russian / Ukrainian
    (["Anastasia", "Ekaterina", "Maria", "Natalia", "Olga", "Sofia",
      "Tatiana", "Yulia", "Irina", "Oksana", "Darya", "Valentina"],
     ["Aleksandr", "Dmitry", "Ivan", "Mikhail", "Nikolai", "Pavel",
      "Sergei", "Vladimir", "Alexei", "Andrei", "Artem", "Kirill"],
     ["Ivanov", "Petrov", "Sidorov", "Kozlov", "Novikov", "Morozov",
      "Volkov", "Sokolov", "Lebedev", "Popov", "Egorov", "Smirnov"]),

    # Polish / Eastern European
    (["Zofia", "Anna", "Julia", "Maja", "Zuzanna", "Natalia",
      "Aleksandra", "Weronika", "Magdalena", "Karolina", "Ewa", "Agnieszka"],
     ["Jakub", "Jan", "Szymon", "Piotr", "Maciej", "Michał",
      "Bartosz", "Tomasz", "Mateusz", "Łukasz", "Paweł", "Marcin"],
     ["Kowalski", "Nowak", "Wiśniewski", "Wójcik", "Kowalczyk",
      "Kaminski", "Lewandowski", "Zielinski", "Szymanski", "Woźniak", "Dąbrowski", "Kozłowski"]),

    # Swedish / Nordic
    (["Astrid", "Britta", "Elsa", "Frida", "Ingrid", "Karin",
      "Maja", "Sigrid", "Linnea", "Ebba", "Klara", "Saga"],
     ["Erik", "Lars", "Johan", "Anders", "Björn", "Sven",
      "Magnus", "Karl", "Nils", "Gustav", "Axel", "Oskar"],
     ["Eriksson", "Larsson", "Pettersson", "Lindqvist", "Gustafsson",
      "Johansson", "Andersson", "Karlsson", "Nilsson", "Svensson", "Persson", "Berg"]),

    # Dutch / Flemish
    (["Eva", "Lotte", "Nora", "Saar", "Fien", "Lies",
      "Roos", "Fleur", "Amber", "Elien", "Jolien", "Silke"],
     ["Daan", "Tim", "Lars", "Sander", "Bas", "Ruben",
      "Robin", "Stef", "Wout", "Pieter", "Niels", "Thijs"],
     ["de Vries", "van den Berg", "Janssen", "Bakker", "Visser",
      "Smit", "Meijer", "de Boer", "Mulder", "van Dijk", "Bos", "Hendriks"]),

    # Vietnamese
    (["Lan", "Huong", "Mai", "Ngoc", "Phuong", "Thuy",
      "Linh", "Thu", "Vy", "Trang", "Hoa", "Nga"],
     ["Minh", "Tuan", "Hoa", "Nam", "Duc", "Long",
      "Hung", "Thanh", "Khoa", "Cuong", "Dat", "Bao"],
     ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Phan",
      "Vu", "Dang", "Bui", "Do", "Ho", "Ngo"]),

    # Filipino
    (["Maria", "Rosa", "Ana", "Luz", "Patricia", "Cristina",
      "Maricel", "Joanna", "Lourdes", "Teresita", "Maribel", "Rosario"],
     ["Juan", "Miguel", "Jose", "Angelo", "Mark", "John",
      "Karl", "Carlo", "Patrick", "Christian", "Rico", "Rommel"],
     ["Reyes", "Cruz", "Santos", "Ramos", "Bautista", "Ocampo",
      "Garcia", "Torres", "Flores", "Aquino", "De Leon", "Villanueva"]),

    # Argentine / South American
    (["Florencia", "Agustina", "Micaela", "Romina", "Silvana",
      "Guadalupe", "Camila", "Luciana", "Celeste", "Soledad", "Vanesa", "Cynthia"],
     ["Matías", "Santiago", "Nicolás", "Facundo", "Sebastián",
      "Leandro", "Esteban", "Gonzalo", "Ramiro", "Ignacio", "Mariano", "Damián"],
     ["Pérez", "González", "Rodríguez", "Fernández", "López",
      "Martínez", "Sánchez", "Romero", "Gómez", "Díaz", "Torres", "Álvarez"]),

    # Iranian / Persian
    (["Shirin", "Narges", "Maryam", "Leila", "Nasrin", "Zahra",
      "Fatemeh", "Atefeh", "Roya", "Hana", "Azadeh", "Mahsa"],
     ["Dariush", "Reza", "Ali", "Mohammad", "Hossein", "Mehdi",
      "Ahmad", "Ehsan", "Arash", "Kamran", "Babak", "Cyrus"],
     ["Ahmadi", "Hosseini", "Rezaei", "Karimi", "Mousavi",
      "Moradi", "Rahimi", "Sadeghi", "Nazari", "Ghorbani", "Ebrahimi", "Shirazi"]),

    # Greek
    (["Eleni", "Maria", "Katerina", "Sophia", "Georgia", "Alexandra",
      "Dimitra", "Christina", "Angeliki", "Ioanna", "Vasiliki", "Panagiota"],
     ["Nikos", "Giorgos", "Kostas", "Vaggelis", "Petros", "Stefanos",
      "Alexandros", "Christos", "Dimitris", "Thanasis", "Manolis", "Spyros"],
     ["Papadopoulos", "Georgiou", "Nikolaou", "Christodoulou", "Andreou",
      "Papageorgiou", "Konstantinou", "Stavrou", "Ioannou", "Makris", "Kyriakidis", "Alexiou"]),
]

# ── name generation ───────────────────────────────────────────────────────────

def _build_name_pool(gender: str, n_needed: int, seed: int = 42) -> list:
    """
    Build a shuffled list of unique full names for 'Female' or 'Male'.
    Draws first names, last names from each cultural group and combines them.
    Guaranteed unique; if pool runs low, expends cross-cultural combos.
    """
    idx = 0 if gender == "Female" else 1
    rng = random.Random(seed)

    # Primary pool: same-culture pairings
    pool = []
    for female_firsts, male_firsts, lasts in _NAME_GROUPS:
        firsts = female_firsts if gender == "Female" else male_firsts
        for fn in firsts:
            for ln in lasts:
                pool.append(f"{fn} {ln}")

    pool = list(dict.fromkeys(pool))   # deduplicate, preserve order
    rng.shuffle(pool)

    # If still not enough, generate cross-cultural combos
    if len(pool) < n_needed:
        all_firsts = []
        all_lasts  = []
        for female_f, male_f, lasts in _NAME_GROUPS:
            all_firsts.extend(female_f if gender == "Female" else male_f)
            all_lasts.extend(lasts)
        extra = []
        for fn in all_firsts:
            for ln in all_lasts:
                name = f"{fn} {ln}"
                if name not in pool:
                    extra.append(name)
        rng.shuffle(extra)
        pool.extend(extra)

    return pool


def assign_names(df: pd.DataFrame, seed: int = 42) -> pd.Series:
    """Assign a unique deterministic name to each persona matching their gender."""
    female_mask = df["gender"] == "Female"
    male_mask   = df["gender"] == "Male"

    n_female = female_mask.sum()
    n_male   = male_mask.sum()

    female_pool = _build_name_pool("Female", n_female, seed=seed)
    male_pool   = _build_name_pool("Male",   n_male,   seed=seed + 1)

    names = pd.Series(index=df.index, dtype=str)
    names[female_mask] = female_pool[:n_female]
    names[male_mask]   = male_pool[:n_male]
    return names


# ── paths ────────────────────────────────────────────────────────────────────
BASE        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SYN         = os.path.join(BASE, "outputs", "data", "synthetic")
META        = os.path.join(BASE, "outputs", "metadata")

PERSONAS_IN  = os.path.join(SYN,  "student_personas.csv")
PROMPTS_OUT  = os.path.join(SYN,  "student_personas_with_prompts.csv")
AUDIT_PATH   = os.path.join(META, "s4_prompt_audit.json")

os.makedirs(META, exist_ok=True)

# ── narrative columns (exclude technical indices) ─────────────────────────────
NARRATIVE_COLS = [
    "persona_archetype_label",
    "age", "gender", "learning_style",
    "internet_access", "extracurricular", "uses_edutech", "resources_availability",
    "study_hours_per_week", "online_courses_enrolled",
    "attendance_pct", "assignment_completion_pct", "exam_score_harmonized",
    "final_grade", "risk_level",
    "avg_response_time_hours", "assignment_score",
    "login_frequency", "video_watch_time_min", "discussion_posts",
    "peer_interaction_count", "task_completion_rate", "engagement_level",
    "motivation_level", "motivation_index",
    "stress_level_label", "stress_score",
    "anxiety_score", "resilience_score", "post_intervention_mood",
    "dominant_emotion", "intervention_type",
]

# ── 24 interview questions from proposal.md Appendix A ───────────────────────
# Domain 1: Study Routines and Engagement Habits
# Domain 2: Perceived Value of LMS Activities
# Domain 3: Engagement Beyond the LMS
# Domain 4: Barriers, Overload, and Disengagement
# Domain 5: Course Structure, Deadlines, and Feedback
# Domain 6: Interpretation of Behavioral Patterns
QUESTIONS = [
    # Domain 1
    ("Q1",  "Can you describe how you usually organized your study routine during the course?"),
    ("Q2",  "Did you tend to study at regular times each week, or did your pattern vary depending on deadlines or other demands?"),
    ("Q3",  "What usually prompted you to log into the LMS?"),
    ("Q4",  "Were there moments when using the LMS became part of your normal routine? If so, how did that happen?"),
    # Domain 2
    ("Q5",  "Which LMS activities or resources felt most useful to your learning? Why?"),
    ("Q6",  "Were there resources you often ignored, delayed, or used only minimally? Why?"),
    ("Q7",  "What made an activity feel worth your time?"),
    ("Q8",  "Did optional activities feel different from required ones? In what way?"),
    # Domain 3
    ("Q9",  "Did you ever study in ways that were not visible in the LMS, such as using downloaded files, notes, textbooks, or external resources?"),
    ("Q10", "Can you describe situations in which you were learning or studying, but your LMS activity would not show much evidence of that?"),
    ("Q11", "Did you ever access materials once and then continue studying them offline?"),
    # Domain 4
    ("Q12", "Were there moments when it became harder to engage with the course? What was happening at that time?"),
    ("Q13", "Did workload, confusion, fatigue, or competing responsibilities affect how you used the LMS?"),
    ("Q14", "Were there times when you accessed materials but did not feel meaningfully engaged with them?"),
    ("Q15", "Did you ever postpone or avoid certain activities even when you knew they were important? Why?"),
    # Domain 5
    ("Q16", "How did deadlines influence the way you used the LMS?"),
    ("Q17", "Did feedback affect how or when you engaged with the course?"),
    ("Q18", "How did the structure of the course influence your participation?"),
    ("Q19", "Were there aspects of the course design that made engagement easier or harder?"),
    # Domain 6
    ("Q20", "Sometimes students show high activity in the LMS but submit little work. What do you think might explain that?"),
    ("Q21", "Sometimes students show low activity in the LMS but still perform reasonably well. Does that reflect anything from your experience?"),
    ("Q22", "What do you think a course log can show accurately about engagement, and what do you think it misses?"),
    ("Q23", "What does 'being engaged' in a course mean to you?"),
    # Closing
    ("Q24", "Is there anything important about your engagement with the course that LMS data would not show?"),
]

# ── JSON schema the LLM must return ──────────────────────────────────────────
# Q1–Q24 as strings + two summary fields
_q_fields = "\n".join(
    f'  "{q[0]}": "<your answer in first person, 3–6 sentences>",'
    for q in QUESTIONS
)
JSON_SCHEMA = f"""{{
  "persona_id": "<copy from profile>",
{_q_fields}
  "overall_engagement_self_assessment": "<one of: high | medium | low>",
  "dominant_themes": ["<theme1>", "<theme2>", "<theme3>"]
}}"""

# ── helpers ──────────────────────────────────────────────────────────────────

def _fmt_pct(v, decimals=1):
    return f"{round(float(v), decimals)}%"

def _fmt_float(v, decimals=2):
    return str(round(float(v), decimals))

def _fmt_int(v):
    return str(int(v))

def _likert_label(v):
    v = float(v)
    if v <= 2.0:   return f"low ({v:.0f}/5)"
    if v <= 3.5:   return f"moderate ({v:.0f}/5)"
    return             f"high ({v:.0f}/5)"


def _h(value) -> str:
    """Replace underscores with spaces for natural-language readability."""
    return str(value).replace("_", " ")


def build_persona_block(row: pd.Series, pid: str) -> str:
    lines = []
    lines.append(f"Student ID     : {pid}")
    lines.append(f"Name           : {row['persona_name']}")
    lines.append(f"Archetype      : {_h(row['persona_archetype_label'])}")
    lines.append("")
    lines.append("── DEMOGRAPHICS ───────────────────────────────────────────")
    lines.append(f"Age            : {_fmt_int(row['age'])} years old")
    lines.append(f"Gender         : {row['gender']}")
    lines.append(f"Learning style : {_h(row['learning_style'])}")
    lines.append(f"Internet access: {_h(row['internet_access'])}")
    lines.append(f"Extracurricular: {_h(row['extracurricular'])}")
    lines.append(f"Uses edutech   : {_h(row['uses_edutech'])}")
    lines.append(f"Resources      : {_h(row['resources_availability'])}")
    lines.append("")
    lines.append("── ACADEMIC PROFILE ────────────────────────────────────────")
    lines.append(f"Study hours/wk : {_fmt_int(row['study_hours_per_week'])} h")
    lines.append(f"Online courses : {_fmt_int(row['online_courses_enrolled'])}")
    lines.append(f"Attendance     : {_fmt_pct(row['attendance_pct'])}")
    lines.append(f"Assignment cpl : {_fmt_pct(row['assignment_completion_pct'])}")
    lines.append(f"Exam score     : {_fmt_float(row['exam_score_harmonized'])}/100")
    lines.append(f"Final grade    : {_h(row['final_grade'])}")
    lines.append(f"Risk level     : {_h(row['risk_level'])}")
    lines.append(f"Avg resp. time : {_fmt_float(row['avg_response_time_hours'])} h")
    lines.append(f"Assignment score: {_fmt_float(row['assignment_score'])}/100")
    lines.append("")
    lines.append("── LMS ENGAGEMENT ──────────────────────────────────────────")
    lines.append(f"Login frequency: {_fmt_int(row['login_frequency'])}")
    lines.append(f"Video watch    : {_fmt_float(row['video_watch_time_min'])} min")
    lines.append(f"Discussion posts: {_fmt_int(row['discussion_posts'])}")
    lines.append(f"Peer interactions: {_fmt_int(row['peer_interaction_count'])}")
    lines.append(f"Task completion: {_fmt_pct(float(row['task_completion_rate'])*100)}")
    lines.append(f"Engagement lvl : {_likert_label(row['engagement_level'])}")
    lines.append("")
    lines.append("── PSYCHOLOGICAL PROFILE ───────────────────────────────────")
    lines.append(f"Motivation     : {row['motivation_level']} (index {_fmt_float(row['motivation_index'])}/5)")
    lines.append(f"Stress         : {row['stress_level_label']} (score {_fmt_float(row['stress_score'])}/5)")
    lines.append(f"Anxiety        : {_likert_label(row['anxiety_score'])}")
    lines.append(f"Resilience     : {_likert_label(row['resilience_score'])}")
    lines.append(f"Post-interv. mood: {_likert_label(row['post_intervention_mood'])}")
    lines.append("")
    lines.append("── CONTEXT ─────────────────────────────────────────────────")
    lines.append(f"Dominant emotion  : {_h(row['dominant_emotion'])}")
    lines.append(f"Intervention type : {_h(row['intervention_type'])}")
    return "\n".join(lines)


def build_prompt(row: pd.Series, pid: str) -> str:
    profile = build_persona_block(row, pid)
    q_block = "\n\n".join(
        f"{tag}. {text}" for tag, text in QUESTIONS
    )
    prompt = f"""You are roleplaying as a university student with the profile below.
Stay fully in character for all 24 answers. Respond as this specific student would —
in first person, grounded in their academic situation, psychological state, and life context.
Do NOT break character. Do NOT add disclaimers, meta-commentary, or notes to the interviewer.

══════════════════════════════════════════════════════════════════════
YOUR PROFILE
══════════════════════════════════════════════════════════════════════
{profile}

══════════════════════════════════════════════════════════════════════
SEMI-STRUCTURED INTERVIEW  (24 questions, 6 domains)
══════════════════════════════════════════════════════════════════════
Answer every question in character. Each answer should be narrative and
personal — 3 to 6 sentences, first person, reflecting this student's
specific background, habits, and emotional state.

{q_block}

══════════════════════════════════════════════════════════════════════
RESPONSE FORMAT
══════════════════════════════════════════════════════════════════════
Return ONLY a valid JSON object. No text before or after it.
Use exactly this schema (all fields required):

{JSON_SCHEMA}

Use "{pid}" as the value for "persona_id".
"dominant_themes" must contain 2–5 short thematic labels
  (e.g., "deadline-driven", "offline-study", "cognitive-overload").
"""
    return prompt.strip()


# ── main ─────────────────────────────────────────────────────────────────────

def _json_safe(obj):
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    return obj


print("█" * 70)
print("  S4 — Generate Persona Prompts  (24 questions, proposal.md Appendix A)")
print("█" * 70)
print()

# [1] Load
print("  [1] Loading student_personas.csv …")
df = pd.read_csv(PERSONAS_IN)
print(f"    Shape: {df.shape}")

missing = [c for c in NARRATIVE_COLS if c not in df.columns]
if missing:
    raise ValueError(f"Missing narrative columns: {missing}")
print(f"    Narrative columns: {len(NARRATIVE_COLS)} ✓")
print(f"    Questions: {len(QUESTIONS)}  (Q1–Q{len(QUESTIONS)}) ✓")
print()

# [1b] Assign persona names
print("  [1b] Assigning persona names …")
df["persona_name"] = assign_names(df)
n_female = (df["gender"] == "Female").sum()
n_male   = (df["gender"] == "Male").sum()
print(f"    {n_female} female + {n_male} male names assigned ✓")
print(f"    Unique names: {df['persona_name'].nunique()} / {len(df)}")
print(df[["persona_id", "gender", "persona_name"]].head(5).to_string(index=False))
print()

# [2] Generate prompts
print("  [2] Generating prompts …")
prompts = []
for i, row in df.iterrows():
    pid = str(row["persona_id"])
    p   = build_prompt(row, pid)
    prompts.append(p)
    if (i + 1) % 100 == 0:
        print(f"    {i+1}/{len(df)} …")

df["persona_prompt"] = prompts
print(f"    Done. Total: {len(prompts)}")
print()

# [3] Validate
print("  [3] Validating …")
assert df["persona_prompt"].isna().sum() == 0
prompt_lens = df["persona_prompt"].str.len()
print(f"    Prompt length: min={prompt_lens.min()}  mean={prompt_lens.mean():.0f}  max={prompt_lens.max()}")
# Spot-check: all Q tags present in first prompt
sample = df.loc[0, "persona_prompt"]
missing_qs = [q[0] for q in QUESTIONS if q[0]+"." not in sample]
if missing_qs:
    raise ValueError(f"Missing question tags in prompt: {missing_qs}")
print(f"    All Q1–Q24 tags present in sample prompt ✓")
print()
print("  Sample (first 12 lines of P0001 prompt):")
for line in sample.split("\n")[:12]:
    print(f"    {line}")
print("    …")
print()

# [4] Save
print("  [4] Saving …")
df.to_csv(PROMPTS_OUT, index=False)
print(f"    {PROMPTS_OUT}")
print(f"    Shape: {df.shape}")
print()

# [5] Audit
audit = {
    "status"         : "COMPLETE",
    "n_personas"     : len(df),
    "n_questions"    : len(QUESTIONS),
    "strategy"       : "one-shot: all 24 questions in a single API call per persona",
    "questions"      : [{"id": q[0], "text": q[1]} for q in QUESTIONS],
    "json_schema_fields": ["persona_id"]
                         + [q[0] for q in QUESTIONS]
                         + ["overall_engagement_self_assessment", "dominant_themes"],
    "prompt_length_stats": {
        "min" : int(prompt_lens.min()),
        "mean": round(float(prompt_lens.mean()), 1),
        "max" : int(prompt_lens.max()),
    },
    "outputs": {"prompts_csv": PROMPTS_OUT},
}
with open(AUDIT_PATH, "w") as f:
    json.dump(_json_safe(audit), f, indent=2)

print(f"  Audit: {AUDIT_PATH}")
print(f"  Status: COMPLETE ✓")
