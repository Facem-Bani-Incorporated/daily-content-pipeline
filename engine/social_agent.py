"""
DailyHistory Social Media Agent
================================
Plugs into the main pipeline as Step 10.
Uses real event data + Cloudinary images with text overlays to generate
ready-to-post social media content.

Usage in main pipeline:
    from engine.social_agent import SocialMediaAgent

    agent = SocialMediaAgent()
    await agent.generate_and_post(final_events_list, today)
"""

import json
import re
import os
import requests as sync_requests
from urllib.parse import quote
import anthropic
from datetime import datetime
from core.config import config
from core.logger import setup_logger

logger = setup_logger("SocialAgent")


def _parse_ai_json(message) -> dict:
    """Extract text blocks from an Anthropic message and parse JSON leniently."""
    text = "".join(
        b.text for b in message.content if getattr(b, "type", None) == "text"
    ).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start, end = text.find("{"), text.rfind("}")
        if start != -1 and end > start:
            return json.loads(text[start:end + 1])
        raise

MAKE_WEBHOOK_URL = os.environ.get("MAKE_WEBHOOK_URL", getattr(config, "MAKE_WEBHOOK_URL", None))
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", getattr(config, "DISCORD_WEBHOOK", None))


class SocialMediaAgent:
    def __init__(self):
        self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY, timeout=600.0)
        self.model = getattr(config, "SOCIAL_AI_MODEL", None) or config.AI_MODEL
        self.thinking_budget = config.AI_THINKING_BUDGET

    # ══════════════════════════════════════════════════════════════
    # CLOUDINARY IMAGE TRANSFORMER
    # ══════════════════════════════════════════════════════════════

    def _build_styled_image_url(self, original_url: str, year: int, title: str, date_str: str) -> str:
        """
        Transform a Cloudinary URL to add text overlays with styling.

        Turns:
          https://res.cloudinary.com/xxx/image/upload/v123/history_app/photo.jpg
        Into:
          https://res.cloudinary.com/xxx/image/upload/[transformations]/v123/history_app/photo.jpg

        Result: A beautiful branded image with year, title, date and app branding.
        """
        if not original_url or "cloudinary" not in original_url:
            return original_url

        # Truncate title if too long (Cloudinary URL has limits)
        display_title = title[:60] + "..." if len(title) > 60 else title

        # Escape special characters for Cloudinary text
        safe_title = display_title.replace(",", "%2C").replace("/", "%2F").replace(":", "%3A").replace("'", "%27")
        safe_date = date_str.replace(",", "%2C").replace(" ", "%20")

        # ── Build transformation chain ──
        transformations = [
            # 1. Resize & crop to Instagram square
            "c_fill,w_1080,h_1080,g_center",

            # 2. Darken the image for text readability
            "e_brightness:-30",

            # 3. Dark gradient overlay at bottom
            "l_fetch:aHR0cHM6Ly9yZXMuY2xvdWRpbmFyeS5jb20vZGVtby9pbWFnZS91cGxvYWQvZV9ncmFkaWVudF9mYWRlLGNfZmlsbCx3XzEwODAsaF81NDAsY29fYmxhY2svYmxhY2tfcmVjdGFuZ2xl",

            # 4. "ON THIS DAY" — small text at top
            f"l_text:Arial_24_bold:{quote('ON THIS DAY')},co_rgb:FFFFFF,o_80,g_north,y_80",

            # 5. YEAR — big bold gold text
            f"l_text:Arial_120_bold:{year},co_rgb:D4A017,g_north,y_120",

            # 6. DATE — below year
            f"l_text:Arial_28:{safe_date},co_rgb:FFFFFF,o_90,g_north,y_260",

            # 7. TITLE — centered, white
            f"l_text:Arial_36_bold:{safe_title},co_rgb:FFFFFF,c_fit,w_900,g_south,y_160",

            # 8. APP BRANDING — bottom
            f"l_text:Arial_20:{quote('Discover more on DailyHistory')},co_rgb:D4A017,o_80,g_south,y_80",

            # 9. Quality optimization
            "q_auto,f_auto",
        ]

        transformation_string = "/".join(transformations)

        # Insert transformations into URL
        # URL format: .../image/upload/v123/folder/file.jpg
        # We need:    .../image/upload/[transforms]/v123/folder/file.jpg
        parts = original_url.split("/upload/")
        if len(parts) == 2:
            styled_url = f"{parts[0]}/upload/{transformation_string}/{parts[1]}"
            return styled_url

        return original_url

    def _build_simple_image_url(self, original_url: str) -> str:
        """Simple version: just resize for Instagram, no text overlay."""
        if not original_url or "cloudinary" not in original_url:
            return original_url

        transformations = "c_fill,w_1080,h_1080,g_center,q_auto,f_auto"
        parts = original_url.split("/upload/")
        if len(parts) == 2:
            return f"{parts[0]}/upload/{transformations}/{parts[1]}"
        return original_url

    def _build_story_image_url(self, original_url: str, year: int, title: str) -> str:
        """Vertical 9:16 format for Stories/TikTok thumbnails."""
        if not original_url or "cloudinary" not in original_url:
            return original_url

        safe_title = title[:50].replace(",", "%2C").replace("/", "%2F").replace(":", "%3A").replace("'", "%27")

        transformations = "/".join([
            "c_fill,w_1080,h_1920,g_center",
            "e_brightness:-25",
            f"l_text:Arial_140_bold:{year},co_rgb:D4A017,g_center,y_-200",
            f"l_text:Arial_40_bold:{safe_title},co_rgb:FFFFFF,c_fit,w_900,g_center,y_100",
            f"l_text:Arial_24:{quote('DailyHistory App')},co_rgb:D4A017,o_80,g_south,y_100",
            "q_auto,f_auto",
        ])

        parts = original_url.split("/upload/")
        if len(parts) == 2:
            return f"{parts[0]}/upload/{transformations}/{parts[1]}"
        return original_url

    # ══════════════════════════════════════════════════════════════
    # MAIN ENTRY POINT
    # ══════════════════════════════════════════════════════════════

    async def generate_and_post(self, events: list, target_date: datetime):
        """
        Main entry point. Called from pipeline after events are finalized.
        """
        if not events:
            logger.warning("⚠️ No events provided to social agent")
            return

        logger.info(f"📱 Social Media Agent — generating posts for {target_date.strftime('%B %d, %Y')}")

        # Step 1: Extract real data from pipeline events
        event_data = self._extract_event_data(events, target_date)

        # Step 2: Generate styled image URLs using Cloudinary transformations
        main = event_data["main_event"]
        date_display = event_data["date_display"]

        # Instagram/Facebook post image (1080x1080 with text overlay)
        styled_post_image = self._build_styled_image_url(
            main["image_url"], main["year"], main["title_en"], date_display
        )
        # Story/vertical format
        styled_story_image = self._build_story_image_url(
            main["image_url"], main["year"], main["title_en"]
        )
        # Clean image without text (for LinkedIn — more professional)
        clean_image = self._build_simple_image_url(main["image_url"])

        logger.info(f"🎨 Styled post image: {styled_post_image[:80]}...")
        logger.info(f"📱 Story image: {styled_story_image[:80]}...")

        # Step 3: Generate social media text content using Claude
        content = self._generate_social_content(event_data, target_date)
        if not content:
            logger.error("❌ Failed to generate social content")
            return

        # Step 4: Attach all image URLs to content
        content["styled_post_image"] = styled_post_image
        content["styled_story_image"] = styled_story_image
        content["clean_image"] = clean_image
        content["raw_image"] = main["image_url"]
        content["all_images"] = event_data["all_images"]

        # Step 5: Send to Make.com
        make_ok = self._send_to_make(content)

        # Step 6: Notify
        self._notify_discord(content, make_ok)

        logger.info(f"📱 Social Agent done — Make.com: {'✅' if make_ok else '⏳'}")

    # ══════════════════════════════════════════════════════════════
    # DATA EXTRACTION
    # ══════════════════════════════════════════════════════════════

    def _extract_event_data(self, events: list, target_date: datetime) -> dict:
        """Extract relevant data from EventDetail objects."""
        main_event = events[0]
        discovery = events[1:5]

        main_image = main_event.gallery[0] if main_event.gallery else ""
        all_images = [ev.gallery[0] for ev in events if ev.gallery]

        data = {
            "date": target_date.strftime("%Y-%m-%d"),
            "date_display": target_date.strftime("%B %d"),
            "main_event": {
                "year": main_event.year,
                "title_en": main_event.title_translations.en,
                "title_ro": main_event.title_translations.ro,
                "narrative_en": main_event.narrative_translations.en[:500],
                "narrative_ro": main_event.narrative_translations.ro[:500],
                "category": main_event.category.value,
                "image_url": main_image,
                "source_url": main_event.source_url,
            },
            "discovery_events": [
                {
                    "year": ev.year,
                    "title_en": ev.title_translations.en,
                    "title_ro": ev.title_translations.ro,
                    "category": ev.category.value,
                    "image_url": ev.gallery[0] if ev.gallery else "",
                }
                for ev in discovery
            ],
            "all_images": all_images,
        }

        logger.info(f"📋 Main event: [{main_event.year}] {main_event.title_translations.en}")
        logger.info(f"🖼️ Main image: {main_image[:80]}..." if main_image else "⚠️ No image")

        return data

    # ══════════════════════════════════════════════════════════════
    # CLAUDE CONTENT GENERATION
    # ══════════════════════════════════════════════════════════════

    def _generate_social_content(self, event_data: dict, target_date: datetime) -> dict:
        """Generate social media posts using Claude, based on REAL event data."""

        main = event_data["main_event"]
        discovery = event_data["discovery_events"]
        date_display = event_data["date_display"]

        discovery_text = "\n".join([
            f"- [{d['year']}] {d['title_en']}" for d in discovery
        ])

        prompt = f"""You are the social media agent for DailyHistory — a mobile app showing daily historical events.

TODAY'S REAL DATA (use ONLY these facts, do NOT invent):

MAIN EVENT:
- Year: {main['year']}
- Title: {main['title_en']}
- Story excerpt: {main['narrative_en'][:400]}
- Category: {main['category']}

OTHER EVENTS TODAY ({date_display}):
{discovery_text}

GENERATE posts for each platform. Tone: curious, surprising, modern — make people stop scrolling.

══════════════════════════════════════════════════════════
CRITICAL SEO & CAPTION RULES — READ CAREFULLY:
══════════════════════════════════════════════════════════

INSTAGRAM CAPTION (MUST be 300-500 words):
- Start with a powerful HOOK line that creates shock or curiosity
- Write a LONG, detailed, storytelling caption — minimum 300 words, ideally 400-500
- Naturally weave in SEO keywords throughout the caption. Keywords to include naturally:
  "today in history", "on this day", "historical event", "what happened today",
  "history facts", "did you know history", "this day in history", "daily history",
  "history of [topic]", "[year] history", "world history", "historical moments"
- Tell the FULL story: what happened, why it mattered, what changed because of it, how it connects to today
- Mention other events from today's date to add more keyword-rich content
- End with an engaging question + "Download DailyHistory app to discover 5 events like this every day"
- MAXIMUM 5 hashtags (Instagram/TikTok limit). Pick the 5 most powerful:
  Example: #todayinhistory #historyfacts #onthisday #worldhistory #dailyhistory
- Use line breaks (\\n\\n) between paragraphs for readability

TIKTOK CAPTION (MUST be 150-300 words):
- Write a LONG descriptive caption, not just a one-liner
- Pack it with SEO keywords: "today in history", "did you know", "historical facts", "on this day in [year]"
- Tell a mini version of the story in the caption itself
- MAXIMUM 5 hashtags
- The caption should work as standalone content even without watching the video

TIKTOK SCRIPT:
- 45-60 second video script
- First 2 seconds = hook that stops scrolling
- Include [VISUAL] cues and text overlay suggestions
- Build tension, end with a twist or mind-blow moment

LINKEDIN (MUST be 200-400 words):
- Extract a BUSINESS/LEADERSHIP lesson from the historical event
- Professional but engaging, thought-leadership style
- Weave in keywords: "leadership lessons", "history teaches us", "innovation", "strategic thinking"
- End with a thought-provoking question to drive comments
- Maximum 5 hashtags

FACEBOOK (MUST be 250-450 words):
- Long-form storytelling style, community-focused
- Tell the complete story with details that make people want to share
- Include keywords naturally: "on this day", "today in history", "did you know"
- Ask a specific question to drive comments
- Mention DailyHistory app naturally

GENERAL RULES:
1. Each platform MUST have COMPLETELY DIFFERENT content — not copy-paste or slight rewording
2. Use ONLY the real events provided — do NOT invent or add events
3. Captions must be LONG and KEYWORD-RICH — short captions are NOT acceptable
4. Maximum 5 hashtags per platform (Instagram and TikTok enforce this)

Return ONLY valid JSON:
{{
  "date": "{event_data['date']}",
  "main_event": {{
    "year": {main['year']},
    "title": "{main['title_en']}"
  }},
  "platforms": {{
    "instagram": {{
      "caption": "LONG 300-500 word caption packed with SEO keywords and storytelling",
      "hashtags": ["todayinhistory", "historyfacts", "onthisday", "worldhistory", "dailyhistory"],
      "content_type": "single"
    }},
    "tiktok": {{
      "script": "Full 45-60s video script with [VISUAL] cues",
      "text_overlays": ["overlay1", "overlay2", "overlay3", "overlay4"],
      "audio_suggestion": "audio type",
      "duration_seconds": 50,
      "caption": "LONG 150-300 word SEO caption with max 5 hashtags at the end"
    }},
    "linkedin": {{
      "post": "200-400 word professional post with leadership angle"
    }},
    "facebook": {{
      "post": "250-450 word storytelling post"
    }}
  }},
  "discovery_teaser": "One-line teaser about the other events today"
}}"""

        try:
            budget = self.thinking_budget
            kwargs = {
                "model": self.model,
                "max_tokens": (budget + 3500) if budget else 3500,
                "system": (
                    "You are a viral social media content creator for a history app. "
                    "Output ONLY valid JSON. No markdown, no code fences."
                ),
                "messages": [{"role": "user", "content": prompt}],
            }
            if budget:
                kwargs["thinking"] = {"type": "enabled", "budget_tokens": budget}
            message = self.client.messages.create(**kwargs)

            content = _parse_ai_json(message)

            usage = message.usage
            logger.info(
                f"📊 Tokens: {usage.input_tokens} + {usage.output_tokens} "
                f"= {usage.input_tokens + usage.output_tokens}"
            )

            return content

        except Exception as e:
            logger.error(f"🚨 Social content generation failed: {e}")
            return None

    # ══════════════════════════════════════════════════════════════
    # MAKE.COM WEBHOOK
    # ══════════════════════════════════════════════════════════════

    def _send_to_make(self, content: dict) -> bool:
        """Send content + styled image URLs to Make.com webhook."""
        if not MAKE_WEBHOOK_URL:
            logger.warning("⚠️ MAKE_WEBHOOK_URL not set — skipping")
            return False

        ig = content["platforms"]["instagram"]
        tt = content["platforms"]["tiktok"]
        li = content["platforms"]["linkedin"]
        fb = content["platforms"]["facebook"]

        payload = {
            # Event info
            "date": content["date"],
            "main_event_year": content["main_event"]["year"],
            "main_event_title": content["main_event"]["title"],

            # ── IMAGE URLs (THE MAGIC) ──
            # Instagram/Facebook: styled image with year, title, branding
            "styled_post_image": content.get("styled_post_image", ""),
            # Stories/TikTok thumbnail: vertical 9:16 with text
            "styled_story_image": content.get("styled_story_image", ""),
            # LinkedIn: clean professional image, no text overlay
            "clean_image": content.get("clean_image", ""),
            # Raw original from Cloudinary
            "raw_image": content.get("raw_image", ""),
            # All event images as JSON array
            "all_image_urls": json.dumps(content.get("all_images", [])),

            # Instagram
            "instagram_caption": ig["caption"] + "\n\n" + " ".join(f"#{t}" for t in ig.get("hashtags", [])),
            "instagram_content_type": ig.get("content_type", "single"),

            # Facebook
            "facebook_post": fb["post"],

            # LinkedIn
            "linkedin_post": li["post"],

            # TikTok
            "tiktok_caption": tt.get("caption", ""),
            "tiktok_script": tt["script"],
            "tiktok_audio": tt.get("audio_suggestion", ""),
            "tiktok_duration": tt.get("duration_seconds", 45),
            "tiktok_overlays": " | ".join(tt.get("text_overlays", [])),

            # Discovery
            "discovery_teaser": content.get("discovery_teaser", ""),
        }

        try:
            response = sync_requests.post(MAKE_WEBHOOK_URL, json=payload, timeout=30)
            if response.status_code == 200:
                logger.info("✅ Sent to Make.com successfully")
                return True
            else:
                logger.error(f"⚠️ Make.com status {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ Make.com error: {e}")
            return False

    # ══════════════════════════════════════════════════════════════
    # DISCORD NOTIFICATION
    # ══════════════════════════════════════════════════════════════

    def _notify_discord(self, content: dict, make_success: bool):
        """Optional Discord notification with image preview."""
        if not DISCORD_WEBHOOK:
            return

        main = content["main_event"]
        status = "✅ Posted" if make_success else "⚠️ Make.com failed"

        embed = {
            "embeds": [{
                "title": f"📱 Social Agent — {content['date']}",
                "description": (
                    f"**{main['year']} — {main['title']}**\n\n"
                    f"**Status:** {status}\n"
                    f"📸 Styled image: ✅\n"
                    f"📱 Story image: ✅\n"
                    f"🖼️ Clean image: ✅"
                ),
                "color": 0x22C55E if make_success else 0xEF4444,
                "image": {"url": content.get("styled_post_image", "")},
            }]
        }

        try:
            sync_requests.post(DISCORD_WEBHOOK, json=embed)
        except Exception:
            pass