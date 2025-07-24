import { useState } from 'react';
import axios from 'axios';

function App() {
  const [query, setQuery] = useState('');
  const [response, setResponse] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const submitQuery = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.post('http://127.0.0.1:8000/query', { query });
      setResponse(res.data);
    } catch (err) {
      setError('Error communicating with backend');
    }
    setLoading(false);
  };

  return (
    <div style={{ padding: '2rem', fontFamily: 'Arial, sans-serif' }}>
      <h1>Air Quality Agent</h1>
      <textarea
        rows={4}
        cols={80}
        placeholder="Ask a question like: 'Which room had the highest temperature last week?'"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        style={{ fontSize: '16px' }}
      />
      <br />
      <button onClick={submitQuery} disabled={loading || !query.trim()}>
        {loading ? 'Loading...' : 'Submit'}
      </button>

      {error && <p style={{ color: 'red' }}>{error}</p>}

      {response && (
        <div style={{ marginTop: '2rem' }}>
          <p><strong>Summary:</strong> {response.summary}</p>
          {response.table && response.table.length > 0 && (
            <table border="1" cellPadding="6" style={{ borderCollapse: 'collapse', marginTop: '1rem' }}>
              <thead>
                <tr>
                  {Object.keys(response.table[0]).map((col) => (
                    <th key={col}>{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {response.table.map((row, idx) => (
                  <tr key={idx}>
                    {Object.values(row).map((val, i) => (
                      <td key={i}>{val}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  );
}

export default App;
