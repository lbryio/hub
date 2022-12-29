import typing
import enum
from typing import Optional
from hub.error import ResolveCensoredError


@enum.unique
class DB_PREFIXES(enum.Enum):
    claim_to_support = b'K'
    support_to_claim = b'L'

    claim_to_txo = b'E'
    txo_to_claim = b'G'

    claim_to_channel = b'I'
    channel_to_claim = b'J'

    claim_short_id_prefix = b'F'
    bid_order = b'D'
    claim_expiration = b'O'

    claim_takeover = b'P'
    pending_activation = b'Q'
    activated_claim_and_support = b'R'
    active_amount = b'S'

    repost = b'V'
    reposted_claim = b'W'

    undo = b'M'
    touched_or_deleted = b'Y'

    tx = b'B'
    block_hash = b'C'
    header = b'H'
    tx_num = b'N'
    tx_count = b'T'
    tx_hash = b'X'
    utxo = b'u'
    hashx_utxo = b'h'
    hashx_history = b'x'
    db_state = b's'
    channel_count = b'Z'
    support_amount = b'a'
    block_tx = b'b'
    trending_notifications = b'c'
    mempool_tx = b'd'
    touched_hashX = b'e'
    hashX_status = b'f'
    hashX_mempool_status = b'g'
    reposted_count = b'j'
    effective_amount = b'i'
    future_effective_amount = b'k'
    hashX_history_hash = b'l'


COLUMN_SETTINGS = {}  # this is updated by the PrefixRow metaclass


# 9/21/2020
MOST_USED_TAGS = {
    "gaming",
    "people & blogs",
    "entertainment",
    "music",
    "pop culture",
    "education",
    "technology",
    "blockchain",
    "news",
    "funny",
    "science & technology",
    "learning",
    "gameplay",
    "news & politics",
    "comedy",
    "bitcoin",
    "beliefs",
    "nature",
    "art",
    "economics",
    "film & animation",
    "lets play",
    "games",
    "sports",
    "howto & style",
    "game",
    "cryptocurrency",
    "playstation 4",
    "automotive",
    "crypto",
    "mature",
    "sony interactive entertainment",
    "walkthrough",
    "tutorial",
    "video game",
    "weapons",
    "playthrough",
    "pc",
    "anime",
    "how to",
    "btc",
    "fun",
    "ethereum",
    "food",
    "travel & events",
    "minecraft",
    "science",
    "autos & vehicles",
    "play",
    "politics",
    "commentary",
    "twitch",
    "ps4live",
    "love",
    "ps4",
    "nonprofits & activism",
    "ps4share",
    "fortnite",
    "xbox",
    "porn",
    "video games",
    "trump",
    "español",
    "money",
    "music video",
    "nintendo",
    "movie",
    "coronavirus",
    "donald trump",
    "steam",
    "trailer",
    "android",
    "podcast",
    "xbox one",
    "survival",
    "audio",
    "linux",
    "travel",
    "funny moments",
    "litecoin",
    "animation",
    "gamer",
    "lets",
    "playstation",
    "bitcoin news",
    "history",
    "xxx",
    "fox news",
    "dance",
    "god",
    "adventure",
    "liberal",
    "2020",
    "horror",
    "government",
    "freedom",
    "reaction",
    "meme",
    "photography",
    "truth",
    "health",
    "lbry",
    "family",
    "online",
    "eth",
    "crypto news",
    "diy",
    "trading",
    "gold",
    "memes",
    "world",
    "space",
    "lol",
    "covid-19",
    "rpg",
    "humor",
    "democrat",
    "film",
    "call of duty",
    "tech",
    "religion",
    "conspiracy",
    "rap",
    "cnn",
    "hangoutsonair",
    "unboxing",
    "fiction",
    "conservative",
    "cars",
    "hoa",
    "epic",
    "programming",
    "progressive",
    "cryptocurrency news",
    "classical",
    "jesus",
    "movies",
    "book",
    "ps3",
    "republican",
    "fitness",
    "books",
    "multiplayer",
    "animals",
    "pokemon",
    "bitcoin price",
    "facebook",
    "sharefactory",
    "criptomonedas",
    "cod",
    "bible",
    "business",
    "stream",
    "comics",
    "how",
    "fail",
    "nsfw",
    "new music",
    "satire",
    "pets & animals",
    "computer",
    "classical music",
    "indie",
    "musica",
    "msnbc",
    "fps",
    "mod",
    "sport",
    "sony",
    "ripple",
    "auto",
    "rock",
    "marvel",
    "complete",
    "mining",
    "political",
    "mobile",
    "pubg",
    "hip hop",
    "flat earth",
    "xbox 360",
    "reviews",
    "vlogging",
    "latest news",
    "hack",
    "tarot",
    "iphone",
    "media",
    "cute",
    "christian",
    "free speech",
    "trap",
    "war",
    "remix",
    "ios",
    "xrp",
    "spirituality",
    "song",
    "league of legends",
    "cat"
}

MATURE_TAGS = [
    'nsfw', 'porn', 'xxx', 'mature', 'adult', 'sex'
]


def normalize_tag(tag):
    return tag.replace(" ", "_").replace("&", "and").replace("-", "_")


COMMON_TAGS = {
    tag: normalize_tag(tag) for tag in list(MOST_USED_TAGS)
}

INDEXED_LANGUAGES = [
  'none',
  'en',
  'aa',
  'ab',
  'ae',
  'af',
  'ak',
  'am',
  'an',
  'ar',
  'as',
  'av',
  'ay',
  'az',
  'ba',
  'be',
  'bg',
  'bh',
  'bi',
  'bm',
  'bn',
  'bo',
  'br',
  'bs',
  'ca',
  'ce',
  'ch',
  'co',
  'cr',
  'cs',
  'cu',
  'cv',
  'cy',
  'da',
  'de',
  'dv',
  'dz',
  'ee',
  'el',
  'eo',
  'es',
  'et',
  'eu',
  'fa',
  'ff',
  'fi',
  'fj',
  'fo',
  'fr',
  'fy',
  'ga',
  'gd',
  'gl',
  'gn',
  'gu',
  'gv',
  'ha',
  'he',
  'hi',
  'ho',
  'hr',
  'ht',
  'hu',
  'hy',
  'hz',
  'ia',
  'id',
  'ie',
  'ig',
  'ii',
  'ik',
  'io',
  'is',
  'it',
  'iu',
  'ja',
  'jv',
  'ka',
  'kg',
  'ki',
  'kj',
  'kk',
  'kl',
  'km',
  'kn',
  'ko',
  'kr',
  'ks',
  'ku',
  'kv',
  'kw',
  'ky',
  'la',
  'lb',
  'lg',
  'li',
  'ln',
  'lo',
  'lt',
  'lu',
  'lv',
  'mg',
  'mh',
  'mi',
  'mk',
  'ml',
  'mn',
  'mr',
  'ms',
  'mt',
  'my',
  'na',
  'nb',
  'nd',
  'ne',
  'ng',
  'nl',
  'nn',
  'no',
  'nr',
  'nv',
  'ny',
  'oc',
  'oj',
  'om',
  'or',
  'os',
  'pa',
  'pi',
  'pl',
  'ps',
  'pt',
  'qu',
  'rm',
  'rn',
  'ro',
  'ru',
  'rw',
  'sa',
  'sc',
  'sd',
  'se',
  'sg',
  'si',
  'sk',
  'sl',
  'sm',
  'sn',
  'so',
  'sq',
  'sr',
  'ss',
  'st',
  'su',
  'sv',
  'sw',
  'ta',
  'te',
  'tg',
  'th',
  'ti',
  'tk',
  'tl',
  'tn',
  'to',
  'tr',
  'ts',
  'tt',
  'tw',
  'ty',
  'ug',
  'uk',
  'ur',
  'uz',
  've',
  'vi',
  'vo',
  'wa',
  'wo',
  'xh',
  'yi',
  'yo',
  'za',
  'zh',
  'zu'
]


class ResolveResult(typing.NamedTuple):
    name: str
    normalized_name: str
    claim_hash: bytes
    tx_num: int
    position: int
    tx_hash: bytes
    height: int
    amount: int
    short_url: str
    is_controlling: bool
    canonical_url: str
    creation_height: int
    activation_height: int
    expiration_height: int
    effective_amount: int
    support_amount: int
    reposted: int
    last_takeover_height: typing.Optional[int]
    claims_in_channel: typing.Optional[int]
    channel_hash: typing.Optional[bytes]
    reposted_claim_hash: typing.Optional[bytes]
    signature_valid: typing.Optional[bool]
    reposted_tx_hash: typing.Optional[bytes]
    reposted_tx_position: typing.Optional[int]
    reposted_height: typing.Optional[int]
    channel_tx_hash: typing.Optional[bytes]
    channel_tx_position: typing.Optional[int]
    channel_height: typing.Optional[int]


class TrendingNotification(typing.NamedTuple):
    height: int
    prev_amount: int
    new_amount: int


class UTXO(typing.NamedTuple):
    tx_num: int
    tx_pos: int
    tx_hash: bytes
    height: int
    value: int


OptionalResolveResultOrError = Optional[typing.Union[ResolveResult, ResolveCensoredError, LookupError, ValueError]]


class ExpandedResolveResult(typing.NamedTuple):
    stream: OptionalResolveResultOrError
    channel: OptionalResolveResultOrError
    repost: OptionalResolveResultOrError
    reposted_channel: OptionalResolveResultOrError


class DBError(Exception):
    """Raised on general DB errors generally indicating corruption."""
