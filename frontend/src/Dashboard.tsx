import { useEffect, useState } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

interface ScoreBucket {
  bucket: string
  count: number
}

interface PassRate {
  task: string
  avg_score: number | null
  attempts: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface FetchState<T> {
  status: 'idle' | 'loading' | 'success' | 'error'
  data: T | null
  error: string | null
}

const API_BASE = ''

function getAuthHeaders(): HeadersInit {
  const apiKey = localStorage.getItem('api_key')
  return {
    Authorization: 'Bearer ' + apiKey,
    'Content-Type': 'application/json',
  }
}

async function fetchWithAuth<T>(url: string): Promise<T> {
  const res = await fetch(url, { headers: getAuthHeaders() })
  if (!res.ok) {
    throw new Error('HTTP ' + res.status)
  }
  return res.json() as Promise<T>
}

export default function Dashboard() {
  const [selectedLab, setSelectedLab] = useState('lab-04')
  const [scoresState, setScoresState] = useState<FetchState<ScoreBucket[]>>({
    status: 'idle',
    data: null,
    error: null,
  })
  const [passRatesState, setPassRatesState] = useState<FetchState<PassRate[]>>({
    status: 'idle',
    data: null,
    error: null,
  })
  const [timelineState, setTimelineState] = useState<FetchState<TimelineEntry[]>>({
    status: 'idle',
    data: null,
    error: null,
  })

  useEffect(() => {
    setScoresState({ status: 'loading', data: null, error: null })
    fetchWithAuth<ScoreBucket[]>(API_BASE + '/analytics/scores?lab=' + selectedLab)
      .then((data) => setScoresState({ status: 'success', data, error: null }))
      .catch((err: Error) =>
        setScoresState({ status: 'error', data: null, error: err.message }),
      )
  }, [selectedLab])

  useEffect(() => {
    setPassRatesState({ status: 'loading', data: null, error: null })
    fetchWithAuth<PassRate[]>(API_BASE + '/analytics/pass-rates?lab=' + selectedLab)
      .then((data) =>
        setPassRatesState({ status: 'success', data, error: null }),
      )
      .catch((err: Error) =>
        setPassRatesState({ status: 'error', data: null, error: err.message }),
      )
  }, [selectedLab])

  useEffect(() => {
    setTimelineState({ status: 'loading', data: null, error: null })
    fetchWithAuth<TimelineEntry[]>(API_BASE + '/analytics/timeline?lab=' + selectedLab)
      .then((data) =>
        setTimelineState({ status: 'success', data, error: null }),
      )
      .catch((err: Error) =>
        setTimelineState({ status: 'error', data: null, error: err.message }),
      )
  }, [selectedLab])

  const scoresChartData =
    scoresState.status === 'success' && scoresState.data
      ? {
          labels: scoresState.data.map((d) => d.bucket),
          datasets: [
            {
              label: 'Number of Students',
              data: scoresState.data.map((d) => d.count),
              backgroundColor: [
                'rgba(255, 99, 132, 0.6)',
                'rgba(255, 159, 64, 0.6)',
                'rgba(75, 192, 192, 0.6)',
                'rgba(54, 162, 235, 0.6)',
              ],
              borderColor: [
                'rgb(255, 99, 132)',
                'rgb(255, 159, 64)',
                'rgb(75, 192, 192)',
                'rgb(54, 162, 235)',
              ],
              borderWidth: 1,
            },
          ],
        }
      : { labels: [], datasets: [] }

  const timelineChartData =
    timelineState.status === 'success' && timelineState.data
      ? {
          labels: timelineState.data.map((d) => d.date),
          datasets: [
            {
              label: 'Submissions',
              data: timelineState.data.map((d) => d.submissions),
              borderColor: 'rgb(54, 162, 235)',
              backgroundColor: 'rgba(54, 162, 235, 0.5)',
              tension: 0.1,
              fill: true,
            },
          ],
        }
      : { labels: [], datasets: [] }

  const commonOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
      },
    },
  }

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h2>Analytics Dashboard</h2>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab: </label>
          <select
            id="lab-select"
            value={selectedLab}
            onChange={(e) => setSelectedLab(e.target.value)}
          >
            <option value="lab-03">Lab 03</option>
            <option value="lab-04">Lab 04</option>
          </select>
        </div>
      </div>

      <div className="charts-container">
        <div className="chart-card">
          <h3>Score Distribution</h3>
          {scoresState.status === 'loading' && <p>Loading...</p>}
          {scoresState.status === 'error' && (
            <p className="error">Error: {scoresState.error}</p>
          )}
          {scoresState.status === 'success' && (
            <Bar
              data={scoresChartData}
              options={{
                ...commonOptions,
                plugins: {
                  ...commonOptions.plugins,
                  title: {
                    display: true,
                    text: 'Scores by Bucket',
                  },
                },
              }}
            />
          )}
        </div>

        <div className="chart-card">
          <h3>Submissions Over Time</h3>
          {timelineState.status === 'loading' && <p>Loading...</p>}
          {timelineState.status === 'error' && (
            <p className="error">Error: {timelineState.error}</p>
          )}
          {timelineState.status === 'success' && (
            <Line
              data={timelineChartData}
              options={{
                ...commonOptions,
                plugins: {
                  ...commonOptions.plugins,
                  title: {
                    display: true,
                    text: 'Daily Submissions',
                  },
                },
              }}
            />
          )}
        </div>
      </div>

      <div className="chart-card">
        <h3>Pass Rates by Task</h3>
        {passRatesState.status === 'loading' && <p>Loading...</p>}
        {passRatesState.status === 'error' && (
          <p className="error">Error: {passRatesState.error}</p>
        )}
        {passRatesState.status === 'success' && passRatesState.data && (
          <table className="pass-rates-table">
            <thead>
              <tr>
                <th>Task</th>
                <th>Avg Score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRatesState.data.map((rate, index) => (
                <tr key={index}>
                  <td>{rate.task}</td>
                  <td>{rate.avg_score?.toFixed(1) ?? 'N/A'}</td>
                  <td>{rate.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
