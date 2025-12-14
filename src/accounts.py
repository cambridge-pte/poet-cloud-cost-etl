"""Account configuration for filtering CUR data."""

ACCOUNTS = {
    # Cambridge Education
    "487940199987": {"name": "cambridge-education-dev"},
    "228210320253": {"name": "cambridge-education-data"},
    "163706035112": {"name": "cambridge-education-dr"},
    "801152766640": {"name": "cambridge-education-lab"},
    "712272638222": {"name": "cambridge-education-sandbox"},
    "983741561209": {"name": "cambridge-education-automation"},

    # Go Global
    "897431556623": {"name": "cambridge-go-prod"},
    "625170045198": {"name": "cambridge-go-staging"},

    # Cambridge Eat
    "764136897737": {"name": "cambridge-eat-prod"},
    "647933391201": {"name": "cambridge-eat-staging"},

    # KC
    "278600754121": {"name": "cambridge-kc-prod"},
    "821495166162": {"name": "cambridge-kc-staging"},

    # PEAS
    "342833786112": {"name": "cambridge-peas-prod"},
    "349717258728": {"name": "cambridge-peas-staging"},

    # Groups
    "268861640710": {"name": "cambridge-groups-prod"},
    "322700963014": {"name": "cambridge-groups-staging"},

    # Edjin
    "319927734414": {"name": "aws-edjin-backup"},
    "471112905727": {"name": "aws-edjin-sandbox"},
    "730335669436": {"name": "aws-edjin-staging"},

    # Special: Sydney region only
    "905174205951": {"name": "cambridge-tng-testhub", "region_filter": "ap-southeast-2"},

    # Other
    "877534089916": {"name": "cup-elevate"},
    "667775550501": {"name": "hotmaths"},
    "369408176762": {"name": "aws-edu"},

    # CEM
    "197394578304": {"name": "cem-admin"},
    "114405749166": {"name": "cem-prd"},

    # Teach Cambridge
    "745774965578": {"name": "teach-cambridge-prod"},
    "659107670883": {"name": "teach-cambridge-nonprod"},
}

def get_account_ids():
    """Return list of all account IDs to filter."""
    return list(ACCOUNTS.keys())

def get_account_name(account_id: str) -> str:
    """Get friendly name for account ID."""
    return ACCOUNTS.get(account_id, {}).get("name", account_id)

def get_region_filter(account_id: str) -> str | None:
    """Get region filter for account if any."""
    return ACCOUNTS.get(account_id, {}).get("region_filter")
