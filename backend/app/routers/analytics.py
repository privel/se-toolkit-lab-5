"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    # Transform lab-04 → Lab 04 for title matching
    lab_title = lab.replace("-", " ").replace("lab", "Lab", 1).capitalize()
    if "lab" in lab_title.lower():
        lab_title = lab_title.replace("lab", "Lab", 1)

    # Find the lab item
    lab_stmt = select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()

    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Find all tasks that belong to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.exec(tasks_stmt)
    task_ids = [task.id for task in tasks_result.all()]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions with scores and group into buckets
    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        (InteractionLog.score <= 100, "76-100"),
        else_="76-100",
    ).label("bucket")

    stmt = (
        select(bucket_case, func.count(InteractionLog.id).label("count"))
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(bucket_case)
    )

    result = await session.exec(stmt)
    rows = result.all()

    # Build result with all buckets
    buckets = {"0-25": 0, "26-50": 0, "51-75": 0, "76-100": 0}
    for bucket, count in rows:
        buckets[bucket] = count

    return [{"bucket": b, "count": c} for b, c in buckets.items()]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    # Transform lab-04 → Lab 04 for title matching
    lab_title = lab.replace("-", " ").replace("lab", "Lab", 1).capitalize()
    if "lab" in lab_title.lower():
        lab_title = lab_title.replace("lab", "Lab", 1)

    # Find the lab item
    lab_stmt = select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.exec(tasks_stmt)
    tasks = tasks_result.all()

    if not tasks:
        return []

    # For each task, compute avg_score and attempts
    results = []
    for task in tasks:
        stmt = (
            select(
                func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
                func.count(InteractionLog.id).label("attempts"),
            )
            .where(InteractionLog.item_id == task.id)
            .where(InteractionLog.score.isnot(None))
        )
        result = await session.exec(stmt)
        row = result.first()
        if row:
            avg_score, attempts = row
            results.append({
                "task": task.title,
                "avg_score": float(avg_score) if avg_score is not None else 0.0,
                "attempts": attempts or 0,
            })

    # Order by task title
    results.sort(key=lambda x: x["task"])
    return results


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    # Transform lab-04 → Lab 04 for title matching
    lab_title = lab.replace("-", " ").replace("lab", "Lab", 1).capitalize()
    if "lab" in lab_title.lower():
        lab_title = lab_title.replace("lab", "Lab", 1)

    # Find the lab item
    lab_stmt = select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.exec(tasks_stmt)
    task_ids = [task.id for task in tasks_result.all()]

    if not task_ids:
        return []

    # Group interactions by date
    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [{"date": str(date), "submissions": count} for date, count in rows]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    # Transform lab-04 → Lab 04 for title matching
    lab_title = lab.replace("-", " ").replace("lab", "Lab", 1).capitalize()
    if "lab" in lab_title.lower():
        lab_title = lab_title.replace("lab", "Lab", 1)

    # Find the lab item
    lab_stmt = select(ItemRecord).where(ItemRecord.title.contains(lab_title))
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()

    if not lab_item:
        return []

    # Find all tasks that belong to this lab
    tasks_stmt = select(ItemRecord).where(ItemRecord.parent_id == lab_item.id)
    tasks_result = await session.exec(tasks_stmt)
    task_ids = [task.id for task in tasks_result.all()]

    if not task_ids:
        return []

    # Join interactions with learners and group by student_group
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(InteractionLog.learner_id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {"group": group, "avg_score": float(avg_score) if avg_score is not None else 0.0, "students": students}
        for group, avg_score, students in rows
    ]
