"""
dbt-branded Textual theme.

Applies a dbt-inspired palette (orange primary, blue secondary) tuned to be
muted and subtle so the UI is pleasant and not overstimulating.

Primary usage is orange (actions, highlights), with a restrained blue used
for secondary accents. Both dark and light variants are included.

Color notes:
- Primary (orange) : warm coral/orange for emphasis
- Secondary (blue): muted blue for secondary accents
- Background: deep neutral on dark theme for low visual noise
"""

from __future__ import annotations

from textual.theme import Theme

dbt_dark_theme = Theme(
    name="dbt-dark",
    # Muted coral/orange as the primary (subtle, not neon)
    primary="#FF7A59",
    # Muted blue as the secondary (calming, used for subtle accents)
    secondary="#4A6FA5",
    # Softer warning / error / success tones for balance
    warning="#D8A53A",
    error="#D85B4A",
    success="#2F9D7E",
    # Use primary for accent by default, but secondary is available for variety
    accent="#FF7A59",
    # Text / background / surfaces tuned for low contrast but good readability
    foreground="#E6E1DB",
    background="#151517",
    surface="#232428",
    panel="#2A2C30",
    dark=True,
    luminosity_spread=0.12,
    text_alpha=0.92,
    variables={
        # cursors: use primary but subtle
        "block-cursor-foreground": "#151517",
        "block-cursor-background": "#FF7A59",
        "input-cursor-foreground": "#151517",
        "input-cursor-background": "#FF7A59",
        # scrollbars: very subtle tinting
        "scrollbar-color": "#FF7A59 18%",
        "scrollbar-color-hover": "#FF7A59 36%",
        "scrollbar-color-active": "#FF7A59 50%",
        # footer
        "footer-background": "#232428",
        "footer-key-foreground": "#FF7A59",
        "footer-description-foreground": "#E6E1DB",
        # secondary accent for things that need calmer emphasis
        "secondary-accent": "#4A6FA5",
    },
)

dbt_light_theme = Theme(
    name="dbt-light",
    # Keep orange primary but slightly softer on light backgrounds
    primary="#E86948",
    # Muted blue secondary for calm accents on light theme
    secondary="#3F63A8",
    warning="#B9842A",
    error="#B84B3A",
    success="#237A63",
    accent="#E86948",
    foreground="#2B2724",
    background="#FBF8F6",
    surface="#F3ECE8",
    panel="#EDE6E1",
    dark=False,
    luminosity_spread=0.12,
    text_alpha=0.90,
    variables={
        "block-cursor-foreground": "#FBF8F6",
        "block-cursor-background": "#E86948",
        "input-cursor-foreground": "#FBF8F6",
        "input-cursor-background": "#E86948",
        "scrollbar-color": "#E86948 20%",
        "scrollbar-color-hover": "#E86948 40%",
        "scrollbar-color-active": "#E86948 60%",
        "footer-background": "#EDE6E1",
        "footer-key-foreground": "#E86948",
        "footer-description-foreground": "#2B2724",
        # secondary accent variable available to styles that want the blue
        "secondary-accent": "#3F63A8",
    },
)

ALL_THEMES: list[Theme] = [dbt_dark_theme, dbt_light_theme]
"""All bundled dbt themes, ready to iterate over for registration."""
