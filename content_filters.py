"""Content filtering lists and helpers with NO database/redis dependency.

Kept separate from ``utils.py`` on purpose: ``utils`` imports ``db`` (Postgres)
at module load, which fails on an ephemeral CI runner. The stateless GitHub
Actions fetcher imports the lists from here instead.

Note: the same lists also exist in ``utils.py`` for the server code. If you edit
one, keep the other in sync (or migrate ``utils`` to import from this module).
"""

# URL fragments that should never be turned into feed items.
BANNED_URL_FRAGMENTS = ["/local", "/states/"]

banned_words = [
    # English keywords
    "killed", "murder", "firing", "rape", "violence", "shoot", "dead", "crime",
    "criminal", "criminals", "shooter", "gun", "weapon", "attack", "molestation",
    "assault", "abuse", "harassment", "trafficking", "beheaded", "beating",
    "bomb", "blast", "terrorist", "terror", "acid attack", "road-accident",
    "numerology", "robbery", "kidnap", "lynching", "hanged", "suicide", "riot",
    "arrested", "jail", "drugs", "assassination",
    "shootout", "execution", "decapitated", "torture", "stabbed", "gangrape", "burnt", "suffocated",
    "domestic violence", "incest", "pedophile", "massacre", "molest",
    "child abuse", "sex scandal", "explicit", "obscene", "lewd", "intoxicated",
    "addiction", "rapist", "threat", "blackmail", "extortion", "drunk driving",
    "violent clash", "hate crime", "slaughter", "corpse", "morgue", "strangulation",
    "sexual abuse", "gang war", "fatal", "fatalities", "bloodshed", "deadly", "disturbing", "beaten-to-death",
    "/local/", "https://www.crictracker.com/live-scores/", "/states/", "local/", "states",

    # Hindi keywords
    "हत्या", "हिंसा", "बलात्कार", "मार डाला", "गंदी हरकत", "सड़क हादसे",
    "हथियार", "आतंकवादी", "आतंक", "बम", "विस्फोट", "फायरिंग", "गोलियां",
    "लूट", "डकैती", "अवैध", "शोषण", "किडनैप", "रेप", "जेल", "गिरफ्तार",
    "फांसी", "आत्महत्या", "दंगा", "मारी गोली", "नशीली दवाइयां",
    "जादू-टोना", "भूत-प्रेत", "अंधविश्वास", "उग्रवादी", "नक्सली", "जिहाद",
    "धमाका", "काट डाला", "तेज़ाब हमला", "जलाकर मारा", "गला घोंटा", "छुरा घोंपा", "गैंगरेप", "नशा", "नशे में",
    "बलात्कारी", "शव", "लाश", "हत्या का मामला", "खून खराबा", "फिरौती",
    "अपहरण", "कालाबाजारी", "मौत", "खतरनाक", "उत्पीड़न", "बलपूर्वक", "धमकी",
    "अशांत", "भयानक", "धर्म विवाद", "हिंसक झड़प", "जघन्य अपराध", "यौन शोषण",
    "बाल शोषण", "भ्रूण हत्या", "नरसंहार", "खून-खराबा", "घातक", "दरिंदा", "अमानवीय",

    # Violence / Crime / Drugs / Death
    "गोली", "मर गया", "मारी", "बच्चा चोर", "गिरोह", "विद्रोह", "बम फेंका",
    "शूटिंग", "गिरफ्तारी", "झगड़ा", "अपराध", "दर्दनाक हादसा",

    # Misc phrases
    "हाथ फेरा", "थाई टच", "सुसाइड",

    # Physical / Personal Harm
    "टक्कर", "एक्सीडेंट", "तड़प-तड़प कर", "खून", "जलाया गया", "सिर कटा",
    "गला दबाया", "खून से लथपथ", "कब्र", "हत्यारा", "हमला", "चाकू", "बुरी तरह से घायल",
]

state_banned_words = [
    "addiction",
]


def is_banned_url(url: str) -> bool:
    """True if the URL path contains a fragment we never want to ingest."""
    low = (url or "").lower()
    return any(frag in low for frag in BANNED_URL_FRAGMENTS)


def has_banned_content(text: str, extra_words=None) -> bool:
    """True if ``text`` contains any banned word (case-insensitive)."""
    low = (text or "").lower()
    words = banned_words if extra_words is None else banned_words + list(extra_words)
    return any(word.lower() in low for word in words)
