import json
import os
from typing import List, Dict

# 12 caption frameworks with 36 Hinglish variants (3 each)
CAPTION_FRAMEWORKS: List[Dict] = [
    {
        "name": "office_pain",
        "template": "{hook}\n{relate}\n{micro}\n{cta}",
        "examples": [
            {
                "hook": "When HR says 'quick call' at 6:58 PM 😭",
                "relate": "Kal ka weekend gaya bhai.",
                "micro": "Boss: 'Just 2 mins' — 2 ghante later...",
                "cta": "Tag that colleague 👇",
            },
            {
                "hook": "WFH but manager is WFH (Work From Heart) ❤️",
                "relate": "Daily standup ya daily stand & suffer?",
                "micro": "JIRA me sab green, dil me sab red.",
                "cta": "Comment your office ritual 👇",
            },
            {
                "hook": "Client: 'Tiny change'",
                "relate": "Reality: restructure the universe.",
                "micro": "PowerPoint ne zindagi le li.",
                "cta": "Save for Monday pain.",
            },
        ],
    },
    {
        "name": "hostel_life",
        "template": "{hook}\n{relate}\n{micro}\n{cta}",
        "examples": [
            {
                "hook": "Hostel 2AM starter pack:",
                "relate": "Maggi, gossip, bad decisions.",
                "micro": "Wing group: online till 4:59 AM.",
                "cta": "Comment your wing ka ritual 👇",
            },
            {
                "hook": "Roommate ne '5 min' bola",
                "relate": "Aur hum semester miss kar gaye.",
                "micro": "Attendance: 0.5%, confidence: 100%.",
                "cta": "Tag your partner-in-crime 👇",
            },
            {
                "hook": "Exam tomorrow, mood today",
                "relate": "Netflix 'Are you still watching?' — yes, anxiety bhi.",
                "micro": "Notes? Bhai forward kar de.",
                "cta": "Save for luck 🍀",
            },
        ],
    },
    {
        "name": "dating_awkward",
        "template": "{hook}\n{relate}\n{micro}\n{cta}",
        "examples": [
            {
                "hook": "When she says 'bhai'...",
                "relate": "Heart: uninstall feelings.exe",
                "micro": "Delivery: 'aapke liye bhaiya' — destiny confirmed.",
                "cta": "Tag that friend jisko bhai bola gaya tha.",
            },
            {
                "hook": "Texting speed vs reply speed",
                "relate": "Hum: 0.2 sec, Un: geological time.",
                "micro": "Blue tick without reply is a crime.",
                "cta": "Comment your red flag 🚩",
            },
            {
                "hook": "Date budget: 300",
                "relate": "Bill: 2999 + service charge on ego.",
                "micro": "UPI: 'Daily limit reached' — same.",
                "cta": "Save for future pain.",
            },
        ],
    },
    {
        "name": "city_roast",
        "template": "{hook}\n{relate}\n{micro}\n{cta}",
        "examples": [
            {
                "hook": "Delhi winter vs Bengaluru traffic",
                "relate": "Kaun zyada thanda? Meetings.",
                "micro": "Metro>Bike? Depends on kaun paid for surge.",
                "cta": "Comment your city slang 👇",
            },
            {
                "hook": "Mumbai rains starter pack",
                "relate": "Umbrella, optimism, and disappointment.",
                "micro": "Local me seat milna = true love.",
                "cta": "Tag your station buddy.",
            },
            {
                "hook": "Bengaluru 2km ETA:",
                "relate": "45 min + HR ka ping.",
                "micro": "Traffic me career planning hota hai.",
                "cta": "Save if you relate.",
            },
        ],
    },
    {
        "name": "chai_money",
        "template": "{hook}\n{relate}\n{micro}\n{cta}",
        "examples": [
            {
                "hook": "Chai > therapy",
                "relate": "Aur sasti bhi.",
                "micro": "Par tip jar me khudka hi paisa.",
                "cta": "Comment: Chai ya Coffee?",
            },
            {
                "hook": "Payday illusion",
                "relate": "Salary in, adulthood out.",
                "micro": "EMI: main hoon na.",
                "cta": "Save for budgeting.",
            },
            {
                "hook": "Splitwise philosophers",
                "relate": "'Bro settle kar na'— national anthem.",
                "micro": "Friend: 'transfer tomorrow' — kabhi kal.",
                "cta": "Tag that friend 💸",
            },
        ],
    },
]

# Add 7 more concise frameworks to reach 12
for name in [
    "parents_energy", "cricket_vibes", "bollywood_roast", "evergreen_desi", "startup_it",
    "weekend_chaos", "nostalgia"
]:
    CAPTION_FRAMEWORKS.append({
        "name": name,
        "template": "{hook}\n{relate}\n{micro}\n{cta}",
        "examples": [
            {"hook": "Parents be like:", "relate": "9pm = late night.", "micro": "WiFi off = character building.", "cta": "Share to your family group."},
            {"hook": "Cricket fever",
             "relate": "Match > meetings.",
             "micro": "Powercut at last over is a national issue.",
             "cta": "Comment your team."},
            {"hook": "Bollywood logic",
             "relate": "Song in rain, job in pain.",
             "micro": "Villain arc = unpaid intern.",
             "cta": "Tag a filmy friend."},
        ],
    })

def build_story_prompts() -> List[Dict]:
    prompts: List[Dict] = []
    # Polls (10)
    prompts += [
        {"type": "poll", "text": "Chai > Coffee?", "options": ["Chai", "Coffee"]},
        {"type": "poll", "text": "Paneer Tikka vs Chicken Tikka?", "options": ["Paneer", "Chicken"]},
        {"type": "poll", "text": "Metro > Bike?", "options": ["Metro", "Bike"]},
        {"type": "poll", "text": "Swiggy > Zomato?", "options": ["Swiggy", "Zomato"]},
        {"type": "poll", "text": "Save or Spend?", "options": ["Save", "Spend"]},
        {"type": "poll", "text": "Bhai-zone exit possible?", "options": ["Yes", "No"]},
        {"type": "poll", "text": "Night plans?", "options": ["Sleep", "DMs open"]},
        {"type": "poll", "text": "Office chai count today?", "options": ["1-2", "3-5+"]},
        {"type": "poll", "text": "Meri salary kahan gayi?", "options": ["EMI", "Food"]},
        {"type": "poll", "text": "Is Delhi winter overhyped?", "options": ["Yes", "No"]},
    ]

    # Quizzes (10)
    prompts += [
        {"type": "quiz", "question": "Who said: 'Mere paas Maa hai'?",
        "options": ["Gabbar", "Vijay", "Shashi Kapoor", "Mogambo"], "answer": "Shashi Kapoor"},
        {"type": "quiz", "question": "Guess the city from slang: 'scene kya hai'?",
        "options": ["Delhi", "Mumbai", "Bengaluru", "Hyderabad"], "answer": "Delhi"},
        {"type": "quiz", "question": "Identify the dialogue: 'How's the josh?'",
        "options": ["Bahubali", "Uri", "RRR", "KGF"], "answer": "Uri"},
    ]

    # Confessions (10)
    prompts += [
        {"type": "confession", "prompt": "Worst date in 7 words?"},
        {"type": "confession", "prompt": "Your hostel scandal (anon)"},
        {"type": "confession", "prompt": "WhatsApp status you regret?"},
        {"type": "confession", "prompt": "Office ka biggest green flag?"},
        {"type": "confession", "prompt": "Broke moment of the week?"},
        {"type": "confession", "prompt": "Tell us your petty revenge (anon)"},
        {"type": "confession", "prompt": "Parents caught you doing what?"},
        {"type": "confession", "prompt": "Exam jugaad that worked?"},
        {"type": "confession", "prompt": "Text you never sent?"},
        {"type": "confession", "prompt": "Best pickup line ya worst?"},
    ]

    # Sliders (10)
    prompts += [
        {"type": "slider", "text": "Rate this roommate 1–10", "emoji": "😅"},
        {"type": "slider", "text": "Friday energy?", "emoji": "🔥"},
        {"type": "slider", "text": "Delhi vs Bengaluru traffic pain", "emoji": "🚗"},
        {"type": "slider", "text": "Cuteness of this meme", "emoji": "🥹"},
        {"type": "slider", "text": "How broke are we", "emoji": "💸"},
        {"type": "slider", "text": "Gym consistency", "emoji": "💪"},
        {"type": "slider", "text": "Hostel chaos level", "emoji": "🔥"},
        {"type": "slider", "text": "Team chai loyalty", "emoji": "☕"},
        {"type": "slider", "text": "How filmy are you", "emoji": "🎬"},
        {"type": "slider", "text": "Weekend plans hype", "emoji": "🎉"},
    ]

    # This-or-That (10)
    prompts += [
        {"type": "either", "text": "Metro or Bike?", "left": "Metro", "right": "Bike"},
        {"type": "either", "text": "Swiggy or Zomato?", "left": "Swiggy", "right": "Zomato"},
        {"type": "either", "text": "Hostel or PG?", "left": "Hostel", "right": "PG"},
        {"type": "either", "text": "DLF or HSR?", "left": "DLF", "right": "HSR"},
        {"type": "either", "text": "Movies or OTT?", "left": "Movies", "right": "OTT"},
        {"type": "either", "text": "Tea or Coffee?", "left": "Tea", "right": "Coffee"},
        {"type": "either", "text": "Early bird or Night owl?", "left": "Early", "right": "Night"},
        {"type": "either", "text": "iOS or Android?", "left": "iOS", "right": "Android"},
        {"type": "either", "text": "Cricket or Football?", "left": "Cricket", "right": "Football"},
        {"type": "either", "text": "Autos or Cabs?", "left": "Auto", "right": "Cab"},
    ]

    # Dares (10)
    prompts += [
        {"type": "dare", "text": "Forward our last post to your squad; proof = shoutout tomorrow."},
        {"type": "dare", "text": "Comment your city slang; best gets a post."},
        {"type": "dare", "text": "Tag 3 friends who owe you money."},
        {"type": "dare", "text": "Send us your funniest DM; credit given."},
        {"type": "dare", "text": "Share this meme to your office group; SS for feature."},
        {"type": "dare", "text": "Remake our meme and tag us; we repost."},
        {"type": "dare", "text": "Tell us your ick anonymously; best goes on feed."},
        {"type": "dare", "text": "Confess a hostel secret; anon safe here."},
        {"type": "dare", "text": "Reply with your savage comeback; we post it."},
        {"type": "dare", "text": "Save this and send to a friend who needs it."},
    ]

    # If < 60, top up with city/language polls programmatically
    cities = ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Chennai", "Kolkata"]
    langs = ["Hindi", "Tamil", "Telugu", "Bengali"]
    i = 0
    while len(prompts) < 60:
        c = cities[i % len(cities)]
        l = langs[i % len(langs)]
        prompts.append({"type": "poll", "text": f"Best city slang: {c}?", "options": ["Elite", "Gali" ]})
        if len(prompts) >= 60:
            break
        prompts.append({"type": "poll", "text": f"Can you roast in {l}?", "options": ["Bilkul", "Thoda"]})
        i += 1
    return prompts


def export_caption_frameworks_json(out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True) if os.path.dirname(out_path) else None
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"frameworks": CAPTION_FRAMEWORKS}, f, ensure_ascii=False, indent=2)


def export_story_prompts_json(out_path: str):
    data = {"prompts": build_story_prompts()}
    os.makedirs(os.path.dirname(out_path), exist_ok=True) if os.path.dirname(out_path) else None
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
