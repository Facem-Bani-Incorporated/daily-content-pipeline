"""
DailyHistory Social Media Agent
================================
Plugs into the main pipeline as Step 10.
Uses real event data + Cloudinary images to generate social media posts.

Usage in main pipeline:
    from engine.social_agent import SocialMediaAgent

    agent = SocialMediaAgent()
    await agent.generate_and_post(final_events_list, today)
"""

import json
import os
import requests as sync_requests
from groq import Groq
from datetime import datetime
from core.config import config
from core.logger import setup_logger

logger = setup_logger("SocialAgent")

MAKE_WEBHOOK_URL = os.environ.get("MAKE_WEBHOOK_URL", getattr(config, "MAKE_WEBHOOK_URL", None))
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", getattr(config, "DISCORD_WEBHOOK", None))


class SocialMediaAgent:
    def __init__(self):
        self.client = Groq(api_key=config.GROQ_API_KEY)
        self.model = getattr(config, "SOCIAL_AI_MODEL", "llama-3.3-70b-versatile")

    async def generate_and_post(self, events: list, target_date: datetime):
        """
        Main entry point. Called from pipeline after events are finalized.

        Args:
            events: list of EventDetail objects (the final top 5)
            target_date: datetime of the events
        """
        if not events:
            logger.warning("⚠️ No events provided to social agent")
            return

        logger.info(f"📱 Social Media Agent — generating posts for {target_date.strftime('%B %d, %Y')}")

        # Step 1: Extract real data from pipeline events
        event_data = self._extract_event_data(events, target_date)

        # Step 2: Generate social media content using Groq
        content = self._generate_social_content(event_data, target_date)
        if not content:
            logger.error("❌ Failed to generate social content")
            return

        # Step 3: Inject real Cloudinary image URLs
        content["main_image_url"] = event_data["main_event"]["image_url"]
        content["all_images"] = event_data["all_images"]

        # Step 4: Send to Make.com
        make_ok = self._send_to_make(content)

        # Step 5: Notify
        self._notify_discord(content, make_ok)

        logger.info(f"📱 Social Agent done — Make.com: {'✅' if make_ok else '⏳'}")

    def _extract_event_data(self, events: list, target_date: datetime) -> dict:
        """
        Extract relevant data from EventDetail objects for the social agent.
        Uses the FIRST event as main (highest score) and rest as discovery.
        """
        main_event = events[0]
        discovery = events[1:5]

        # Get the best image from gallery (first = hero from Pexels)
        main_image = main_event.gallery[0] if main_event.gallery else ""

        all_images = []
        for ev in events:
            if ev.gallery:
                all_images.append(ev.gallery[0])

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
        logger.info(f"📸 Total images available: {len(all_images)}")

        return data

    def _generate_social_content(self, event_data: dict, target_date: datetime) -> dict:
        """Generate social media posts using Groq, based on REAL event data."""

        main = event_data["main_event"]
        discovery = event_data["discovery_events"]
        date_display = event_data["date_display"]

        discovery_text = "\n".join([
            f"- [{d['year']}] {d['title_en']}" for d in discovery
        ])

        prompt = f"""You are the social media agent for DailyHistory — a mobile app showing daily historical events.

TODAY'S REAL DATA (from our pipeline — use ONLY these facts, do NOT invent):

MAIN EVENT:
- Year: {main['year']}
- Title: {main['title_en']}
- Story: {main['narrative_en'][:400]}
- Category: {main['category']}

OTHER EVENTS TODAY ({date_display}):
{discovery_text}

GENERATE social media posts for each platform. Tone: curious, surprising, modern — like a friend who knows cool history facts. Make people stop scrolling.

RULES:
1. Use ONLY the real events above — do NOT add or invent events
2. Each platform gets DIFFERENT content (not copy-paste)
3. Mention "DailyHistory app" naturally
4. The main event should be the star of Instagram, TikTok, and Facebook
5. LinkedIn should extract a business/leadership lesson from the main event

Return ONLY valid JSON:
{{
  "date": "{event_data['date']}",
  "main_event": {{
    "year": {main['year']},
    "title": "{main['title_en']}"
  }},
  "platforms": {{
    "instagram": {{
      "caption": "Full caption with \\n\\n for line breaks",
      "hashtags": ["history", "todayinhistory", "tag3"],
      "content_type": "single"
    }},
    "tiktok": {{
      "script": "Full video script with [VISUAL] cues",
      "text_overlays": ["overlay1", "overlay2"],
      "audio_suggestion": "audio type",
      "duration_seconds": 45,
      "caption": "Short TikTok caption with hashtags"
    }},
    "linkedin": {{
      "post": "Full LinkedIn post"
    }},
    "facebook": {{
      "post": "Full Facebook post"
    }}
  }},
  "discovery_teaser": "One-line teaser about the other events today"
}}"""

        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a social media content creator. Output ONLY valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=3500,
                response_format={"type": "json_object"},
            )

            text = completion.choices[0].message.content.strip()
            content = json.loads(text)

            usage = completion.usage
            logger.info(f"📊 Tokens: {usage.prompt_tokens} + {usage.completion_tokens} = {usage.total_tokens}")

            return content

        except Exception as e:
            logger.error(f"🚨 Social content generation failed: {e}")
            return None

    def _send_to_make(self, content: dict) -> bool:
        """Send content + Cloudinary image URL to Make.com webhook."""
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

            # The real Cloudinary image URL from your pipeline!
            "image_url": content.get("main_image_url", ""),
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

            # Discovery teaser
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

    def _notify_discord(self, content: dict, make_success: bool):
        """Optional Discord notification."""
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
                    f"📸 Image: {'✅' if content.get('main_image_url') else '❌'}\n"
                    f"📱 IG + FB + LI + TikTok script"
                ),
                "color": 0x22C55E if make_success else 0xEF4444,
                "thumbnail": {"url": content.get("main_image_url", "")},
            }]
        }

        try:
            sync_requests.post(DISCORD_WEBHOOK, json=embed)
        except Exception:
            pass