from datetime import datetime

from sqlalchemy import delete, func, select, update

from botasaurus import bt

from .cleaners import normalize_dicts_by_fieldnames
from .db_setup import AsyncSession
from .models import Task, TaskStatus, remove_duplicates_by_key


class TaskHelper:
    @staticmethod
    async def get_completed_children_results(
        session: AsyncSession,
        parent_id: int,
        except_task_id: int | None = None,
        remove_duplicates_by: str | None = None,
    ):
        query = select(Task.result).where(
            Task.parent_task_id == parent_id,
            Task.status == TaskStatus.COMPLETED,
        )
        if except_task_id:
            query = query.where(Task.id != except_task_id)
        result = await session.execute(query)
        results = result.scalars().all()

        all_results = bt.flatten(results)
        rs = normalize_dicts_by_fieldnames(all_results)

        if remove_duplicates_by:
            rs = remove_duplicates_by_key(rs, remove_duplicates_by)
        return rs

    @staticmethod
    async def are_all_child_task_done(session: AsyncSession, parent_id: int):
        done_children_count = await TaskHelper.get_done_children_count(
            session,
            parent_id,
        )
        child_count = await TaskHelper.get_all_children_count(
            session,
            parent_id,
        )

        return done_children_count == child_count

    @staticmethod
    async def get_all_children_count(
        session: AsyncSession,
        parent_id: int,
        except_task_id: int | None = None,
    ):
        query = (
            select(func.count())
            .select_from(Task)
            .where(Task.parent_task_id == parent_id)
        )
        if except_task_id:
            query = query.where(Task.id != except_task_id)

        return await session.scalar(query)

    @staticmethod
    async def get_done_children_count(
        session: AsyncSession,
        parent_id: int,
        except_task_id: int | None = None,
    ):
        query = (
            select(func.count())
            .select_from(Task)
            .where(
                Task.parent_task_id == parent_id,
                Task.status.in_(
                    [
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.ABORTED,
                    ]
                ),
            )
        )
        if except_task_id:
            query = query.where(Task.id != except_task_id)

        return await session.scalar(query)

    @staticmethod
    async def is_task_completed_or_failed(session: AsyncSession, task_id):
        query = select(Task.id).where(
            Task.id == task_id,
            Task.status.in_(
                [
                    TaskStatus.COMPLETED,
                    TaskStatus.FAILED,
                ]
            ),
        )
        result = await session.execute(query)
        return result.scalars().first() is not None

    @staticmethod
    async def get_pending_or_executing_child_count(
        session: AsyncSession,
        parent_id: int,
        except_task_id: int | None = None,
    ):
        query = (
            select(func.count())
            .select_from(Task)
            .where(
                Task.parent_task_id == parent_id,
                Task.status.in_(
                    [TaskStatus.PENDING, TaskStatus.IN_PROGRESS],
                ),
            )
        )
        if except_task_id:
            query = query.where(Task.id != except_task_id)

        return await session.scalar(query)

    @staticmethod
    async def get_failed_children_count(
        session: AsyncSession,
        parent_id: int,
        except_task_id: int | None = None,
    ):
        query = (
            select(func.count())
            .select_from(Task)
            .where(
                Task.parent_task_id == parent_id,
                Task.status == TaskStatus.FAILED,
            )
        )
        if except_task_id:
            query = query.where(Task.id != except_task_id)

        return await session.scalar(query)

    @staticmethod
    async def get_aborted_children_count(
        session: AsyncSession,
        parent_id: int,
        except_task_id: int | None = None,
    ):
        query = (
            select(func.count())
            .select_from(Task)
            .where(
                Task.parent_task_id == parent_id,
                Task.status == TaskStatus.ABORTED,
            )
        )
        if except_task_id:
            query = query.where(Task.id != except_task_id)

        return await session.scalar(query)

    @staticmethod
    async def delete_task(
        session: AsyncSession, task_id: int, is_all_task: bool
    ):
        await session.execute(
            delete(Task).where(Task.id == task_id),
        )

    @staticmethod
    async def delete_child_tasks(session: AsyncSession, task_id: int):
        await session.execute(
            delete(Task).where(Task.parent_task_id == task_id),
        )

    @staticmethod
    async def update_task(
        session: AsyncSession,
        task_id: int,
        data: dict,
        in_status: list[TaskStatus] | None = None,
    ):
        query = update(Task).where(Task.id == task_id)
        if in_status:
            query = query.where(Task.status.in_(in_status))

        query = query.values(**data)
        return await session.execute(query)

    @staticmethod
    async def abort_task(session: AsyncSession, task_id: int):
        query = (
            update(Task)
            .where(
                Task.id == task_id,
                Task.finished_at.is_(None),
            )
            .values({"finished_at": datetime.now()})
        )
        await session.execute(query)

        return TaskHelper.update_task(
            session,
            task_id,
            {
                "status": TaskStatus.ABORTED,
            },
        )

    @staticmethod
    async def abort_child_tasks(session: AsyncSession, task_id: int):
        query = (
            update(Task)
            .where(
                Task.parent_task_id == task_id,
                Task.finished_at.is_(None),
            )
            .values({"finished_at": datetime.now()})
        )
        await session.execute(query)

        query = (
            update(Task)
            .where(
                Task.parent_task_id == task_id,
                Task.finished_at.is_(None),
            )
            .values({"status": TaskStatus.ABORTED})
        )
        await session.execute(query)

    @staticmethod
    async def collect_and_save_all_task(
        session: AsyncSession,
        parent_id: int,
        except_task_id: int | None = None,
        remove_duplicates_by: str | None = None,
        status: TaskStatus | None = None,
    ):
        all_results = await TaskHelper.get_completed_children_results(
            session,
            parent_id,
            except_task_id,
            remove_duplicates_by,
        )
        # TaskResults.save_all_task(parent_id, all_results)

        await TaskHelper.update_task(
            session,
            parent_id,
            {
                "result_count": len(all_results),
                "status": status,
                "finished_at": datetime.now(),
                "result": all_results,
            },
        )
        await session.commit()

    # @staticmethod
    # async def read_clean_save_task(parent_id, remove_duplicates_by, status):
    #     rs = TaskResults.get_all_task(parent_id) or []
    #     rs = normalize_dicts_by_fieldnames(rs)

    #     if remove_duplicates_by:
    #         rs = remove_duplicates_by_key(rs, remove_duplicates_by)
    #     TaskResults.save_all_task(parent_id, rs)

    #     async with AsyncSessionMaker() as session:
    #         await TaskHelper.update_task(
    #             session,
    #             parent_id,
    #             {
    #                 # update with result length after removing duplicates
    #                 "result_count": len(rs),
    #                 "status": status,
    #                 "finished_at": datetime.now(),
    #             },
    #         )
    #         await session.commit()

    @staticmethod
    async def update_parent_task_results(session: AsyncSession, parent_id, result):
        from sqlalchemy import text
        await session.execute(
            update(Task)
            .where(Task.id == parent_id)
            .values(
                result_count=Task.result_count + len(result),
                result=text(
                    "COALESCE(result, '[]'::json) || :new_result::json"
                )
            ),
            {"new_result": result}
        )
        await session.commit()

    @staticmethod
    async def get_task(
        session: AsyncSession,
        task_id: int,
        in_status: list[TaskStatus] | None = None,
    ) -> Task | None:
        if in_status:
            return (
                (
                    await session.execute(
                        select(Task).where(
                            Task.id == task_id,
                            Task.status.in_(in_status),
                        ),
                    )
                )
                .scalars()
                .first()
            )
        else:
            return await session.get(Task, task_id)

    @staticmethod
    async def get_tasks_with_entities(
        session: AsyncSession,
        task_ids: list[int],
        entities,
    ) -> list[Task]:
        result = await session.execute(
            select(*entities).where(Task.id.in_(task_ids)),
        )
        return result.all()

    @staticmethod
    async def get_tasks_by_ids(session: AsyncSession, task_ids) -> list[Task]:
        result = await session.execute(
            select(Task).where(Task.id.in_(task_ids)),
        )
        return result.scalars().all()
