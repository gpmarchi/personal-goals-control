import dayjs from 'dayjs'
import weekOfYear from 'dayjs/plugin/weekOfYear'
import { and, count, eq, gte, lte, sql } from 'drizzle-orm'
import { db } from '../db'
import { goalCompletions, goals } from '../db/schema'

interface CreateGoalCompletionRequest {
  goalId: string
}

dayjs.extend(weekOfYear)

export async function createGoalCompletion({
  goalId,
}: CreateGoalCompletionRequest) {
  const firstDayOfWeek = dayjs().startOf('week').toDate()
  const lastDayOfWeek = dayjs().endOf('week').toDate()

  const goalCompletionCountCurrentWeek = db.$with('goals_completion_counts').as(
    db
      .select({
        goalId: goalCompletions.goalId,
        completionsCount: count(goalCompletions.id).as('completionsCount'),
      })
      .from(goalCompletions)
      .where(
        and(
          gte(goalCompletions.createdAt, firstDayOfWeek),
          lte(goalCompletions.createdAt, lastDayOfWeek),
          eq(goalCompletions.goalId, goalId)
        )
      )
      .groupBy(goalCompletions.goalId)
  )

  const result = await db
    .with(goalCompletionCountCurrentWeek)
    .select({
      desiredWeeklyFrequency: goals.desiredWeeklyFrequency,
      completionsCount: sql`
        COALESCE(${goalCompletionCountCurrentWeek.completionsCount}, 0)
      `.mapWith(Number),
    })
    .from(goals)
    .leftJoin(
      goalCompletionCountCurrentWeek,
      eq(goalCompletionCountCurrentWeek.goalId, goals.id)
    )
    .where(eq(goals.id, goalId))
    .limit(1)

  const { completionsCount, desiredWeeklyFrequency } = result[0]

  if (completionsCount >= desiredWeeklyFrequency) {
    throw new Error('Goal already completed this week.')
  }

  const insertResult = await db
    .insert(goalCompletions)
    .values({ goalId })
    .returning()
  const goalCompletion = insertResult[0]

  return {
    goalCompletion,
  }
}
