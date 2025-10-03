from __future__ import annotations

import json
import os
from typing import Iterable

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Prefetch

from dataset_imports.models import ConversationRecord, DatasetImport, MessageRecord


def _iter_conversations(import_obj: DatasetImport, *, limit: int, randomize: bool) -> Iterable[ConversationRecord]:
    qs = (
        ConversationRecord.objects.filter(dataset_import=import_obj)
        .prefetch_related(Prefetch("messages", queryset=MessageRecord.objects.order_by("turn_index")))
    )
    if randomize:
        qs = qs.order_by("?")
    else:
        qs = qs.order_by("id")
    return qs[: max(0, int(limit))]


class Command(BaseCommand):
    help = "Sample normalized conversations to static/dev/dev_messages.jsonl for Dev Dataset."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dataset-id",
            dest="dataset_id",
            default="lmsys_chat_1m",
            help="Source dataset identifier (default: lmsys_chat_1m)",
        )
        parser.add_argument(
            "--limit",
            dest="limit",
            type=int,
            default=1000,
            help="Number of conversations to export (default: 1000)",
        )
        parser.add_argument(
            "--output",
            dest="output",
            default=os.path.join("static", "dev", "dev_messages.jsonl"),
            help="Output JSONL path (default: static/dev/dev_messages.jsonl)",
        )
        parser.add_argument(
            "--random",
            dest="randomize",
            action="store_true",
            help="Randomize selection (uses ORDER BY ?; fine for dev, not for very large tables)",
        )

    def handle(self, *args, **options):
        dataset_id: str = options["dataset_id"]
        limit: int = int(options["limit"])
        output: str = str(options["output"])
        randomize: bool = bool(options["randomize"])

        if limit <= 0:
            raise CommandError("--limit must be > 0")

        import_obj = (
            DatasetImport.objects.filter(notes__dataset_id=dataset_id)
            .order_by("-created_at")
            .first()
        )
        if not import_obj:
            raise CommandError(
                f"No DatasetImport found for dataset_id='{dataset_id}'. Run an import + normalization first."
            )

        conversations = list(_iter_conversations(import_obj, limit=limit, randomize=randomize))
        if not conversations:
            raise CommandError(
                "No conversations found. Ensure normalization completed and try a larger limit."
            )

        out_dir = os.path.dirname(output)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir, exist_ok=True)

        count = 0
        with open(output, "w", encoding="utf-8") as handle:
            for conv in conversations:
                payload = {
                    "id": conv.source_id or str(conv.pk),
                    "messages": [
                        {
                            "role": msg.role,
                            "content": msg.content,
                        }
                        for msg in conv.messages.all()
                    ],
                }
                handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
                count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Wrote {count} conversation(s) from '{dataset_id}' to {output}"
            )
        )

