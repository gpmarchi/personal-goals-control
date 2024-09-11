import dayjs from 'dayjs'
import weekOfYear from 'dayjs/plugin/weekOfYear'
import { and, count, eq, gte, lte, sql } from 'drizzle-orm'
import { db } from '../db'
import { goalCompletions, goals } from '../db/schema'

dayjs.extend(weekOfYear)

export async function getWeekPendingGoals() {
  const firstDayOfWeek = dayjs().startOf('week').toDate()
  const lastDayOfWeek = dayjs().endOf('week').toDate()

  const goalsCreatedUpToCurrentWeek = db
    .$with('goals_created_up_to_current_week')
    .as(
      db
        .select({
          id: goals.id,
          title: goals.title,
          desiredWeeklyFrequency: goals.desiredWeeklyFrequency,
          createdAt: goals.createdAt,
        })
        .from(goals)
        .where(lte(goals.createdAt, lastDayOfWeek))
    )

  const goalCompletionCounts = db.$with('goals_completion_counts').as(
    db
      .select({
        goalId: goalCompletions.goalId,
        completionsCount: count(goalCompletions.id).as('completionsCount'),
      })
      .from(goalCompletions)
      .where(
        and(
          gte(goalCompletions.createdAt, firstDayOfWeek),
          lte(goalCompletions.createdAt, lastDayOfWeek)
        )
      )
      .groupBy(goalCompletions.goalId)
  )

  const pendingGoals = await db
    .with(goalsCreatedUpToCurrentWeek, goalCompletionCounts)
    .select({
      id: goalsCreatedUpToCurrentWeek.id,
      title: goalsCreatedUpToCurrentWeek.title,
      desiredWeeklyFrequency:
        goalsCreatedUpToCurrentWeek.desiredWeeklyFrequency,
      completionsCount: sql`
        COALESCE(${goalCompletionCounts.completionsCount}, 0)
      `.mapWith(Number),
    })
    .from(goalsCreatedUpToCurrentWeek)
    .leftJoin(
      goalCompletionCounts,
      eq(goalCompletionCounts.goalId, goalsCreatedUpToCurrentWeek.id)
    )

  return {
    pendingGoals,
  }
}
