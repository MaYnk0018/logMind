# 06 — Phase 6: API Gateway + React Frontend

> **Goal**: Single HTTP entry point. React dashboard with live log stream, anomaly explorer, incident cards, alert config.
>
> **New services**: `api-gateway` :3000, React frontend :5173
>
> **Estimated time**: 24–30 hours

---

## 1. API Gateway — Spring Cloud Gateway

### `pom.xml`
```xml
<dependency>
  <groupId>org.springframework.cloud</groupId>
  <artifactId>spring-cloud-starter-gateway</artifactId>
</dependency>
```

### `GatewayRouteConfig.java` — `@Configuration`

```java
@Configuration
public class GatewayRouteConfig {

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder) {
        return builder.routes()
            .route("ingestion", r -> r.path("/api/logs/**")
                .filters(f -> f.stripPrefix(1))
                .uri("http://ingestion-service:3001"))
            .route("anomalies", r -> r.path("/api/anomalies/**")
                .filters(f -> f.stripPrefix(1))
                .uri("http://anomaly-detection-service:3003"))
            .route("incidents", r -> r.path("/api/incidents/**")
                .filters(f -> f.stripPrefix(1))
                .uri("http://ai-analysis-service:3004"))
            .route("alerts", r -> r.path("/api/alerts/**")
                .filters(f -> f.stripPrefix(1))
                .uri("http://alert-service:3005"))
            .build();
    }
}
```

### `SseLogStreamHandler.java` — `@Component`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class SseLogStreamHandler {

    private final KafkaConsumerFactory<String, StoredLogMessage> consumerFactory;

    // Thread-safe: multiple React clients connect simultaneously
    private final CopyOnWriteArrayList<SseEmitter> clients = new CopyOnWriteArrayList<>();

    @GetMapping("/api/stream/logs")
    public SseEmitter streamLogs() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        clients.add(emitter);

        emitter.onCompletion(() -> clients.remove(emitter));
        emitter.onTimeout(() -> clients.remove(emitter));
        emitter.onError(e -> clients.remove(emitter));

        return emitter;
    }

    // Called by a background thread polling stored-logs
    public void broadcast(StoredLogMessage log) {
        List<SseEmitter> dead = new ArrayList<>();
        for (SseEmitter client : clients) {
            try {
                client.send(SseEmitter.event()
                        .name("log")
                        .data(log));
            } catch (IOException e) {
                dead.add(client);
            }
        }
        clients.removeAll(dead);
    }
}
```

**Why SSE not WebSocket?**
- SSE is HTTP/1.1 compatible — works through all proxies and load balancers
- Auto-reconnect built into the browser `EventSource` API
- One-directional (server→client) — no need for bidirectional for a log stream
- No upgrade handshake overhead

---

## 2. React Frontend — component LLD

### Project structure
```
frontend/
├── src/
│   ├── api/
│   │   ├── logs.js          getLogs(), getServices(), getTimeseries()
│   │   ├── anomalies.js     getAnomalies(), getAnomalyById()
│   │   ├── incidents.js     getIncidentByAnomalyId(), submitFeedback(), getAccuracy()
│   │   └── alerts.js        getAlertRules(), createRule(), updateRule(), toggleRule(), deleteRule()
│   ├── components/
│   │   ├── LogStream.jsx        live SSE log viewer
│   │   ├── AnomalyList.jsx      paginated table, row click → IncidentCard
│   │   ├── IncidentCard.jsx     hypothesis, confidence bar, actions, feedback
│   │   ├── FeedbackButtons.jsx  thumbs up/down + optional notes
│   │   ├── ErrorRateChart.jsx   Recharts LineChart per-service error rate
│   │   └── AlertRuleForm.jsx    create/edit rule form
│   └── pages/
│       ├── Dashboard.jsx        LogStream + ErrorRateChart + recent anomalies
│       ├── IncidentExplorer.jsx AnomalyList + IncidentCard side panel
│       └── AlertConfig.jsx      AlertRules CRUD
```

### `LogStream.jsx`

```jsx
function LogStream() {
  const [logs, setLogs] = useState([]);
  const bottomRef = useRef(null);
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    const es = new EventSource('/api/stream/logs');
    es.addEventListener('log', (e) => {
      const log = JSON.parse(e.data);
      setLogs(prev => [...prev.slice(-199), log]);  // rolling 200 lines
    });
    es.onerror = () => es.close();  // browser auto-reconnects on next render
    return () => es.close();
  }, []);

  useEffect(() => {
    if (autoScroll) bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs, autoScroll]);

  const levelColor = {
    DEBUG: 'text-gray-400',
    INFO:  'text-blue-400',
    WARN:  'text-yellow-400',
    ERROR: 'text-red-400',
    FATAL: 'text-red-600 font-bold',
  };

  return (
    <div className="bg-gray-900 rounded-lg p-4 h-96 overflow-y-auto font-mono text-sm">
      {logs.map((log, i) => (
        <div key={i} className={`${levelColor[log.level]} mb-0.5`}>
          <span className="text-gray-500">{log.timestamp}</span>
          {' '}
          <span>[{log.level}]</span>
          {' '}
          <span className="text-white">{log.message}</span>
        </div>
      ))}
      <div ref={bottomRef} />
    </div>
  );
}
```

### `IncidentCard.jsx`

```jsx
function IncidentCard({ anomalyId }) {
  const [incident, setIncident] = useState(null);

  useEffect(() => {
    if (!anomalyId) return;
    getIncidentByAnomalyId(anomalyId).then(setIncident);
  }, [anomalyId]);

  if (!incident) return <div>Loading AI analysis...</div>;

  return (
    <div className="p-6 space-y-4">
      <div>
        <h3 className="font-semibold text-gray-900">Root Cause Hypothesis</h3>
        <p className="text-gray-700 mt-1">{incident.hypothesis}</p>
      </div>

      <div>
        <h4 className="text-sm font-medium text-gray-500">Confidence</h4>
        <div className="w-full bg-gray-200 rounded-full h-2 mt-1">
          <div
            className="bg-blue-600 h-2 rounded-full"
            style={{ width: `${incident.confidence * 100}%` }}
          />
        </div>
        <span className="text-xs text-gray-500">{Math.round(incident.confidence * 100)}%</span>
      </div>

      <div>
        <h4 className="text-sm font-medium text-gray-500">Affected Components</h4>
        <div className="flex flex-wrap gap-2 mt-1">
          {incident.affectedComponents.map(c => (
            <span key={c} className="bg-blue-100 text-blue-800 text-xs px-2 py-1 rounded">
              {c}
            </span>
          ))}
        </div>
      </div>

      <div>
        <h4 className="text-sm font-medium text-gray-500">Suggested Actions</h4>
        <ol className="list-decimal list-inside space-y-1 mt-1">
          {incident.suggestedActions.map((a, i) => (
            <li key={i} className="text-gray-700 text-sm">{a}</li>
          ))}
        </ol>
      </div>

      <FeedbackButtons incidentId={incident.id} />
    </div>
  );
}
```

### `FeedbackButtons.jsx`

```jsx
function FeedbackButtons({ incidentId }) {
  const [submitted, setSubmitted] = useState(false);
  const [notes, setNotes] = useState('');
  const [showNotes, setShowNotes] = useState(false);
  const [pendingRating, setPendingRating] = useState(null);

  const handleClick = (rating) => {
    setPendingRating(rating);
    setShowNotes(true);
  };

  const handleSubmit = async () => {
    await submitFeedback(incidentId, pendingRating, notes);
    setSubmitted(true);
  };

  if (submitted) return <p className="text-green-600 text-sm">Thanks for your feedback!</p>;

  return (
    <div className="space-y-2">
      {!showNotes ? (
        <div className="flex gap-2">
          <button onClick={() => handleClick(1)}
            className="flex items-center gap-1 px-3 py-1.5 rounded border hover:bg-green-50">
            👍 Helpful
          </button>
          <button onClick={() => handleClick(-1)}
            className="flex items-center gap-1 px-3 py-1.5 rounded border hover:bg-red-50">
            👎 Not helpful
          </button>
        </div>
      ) : (
        <div className="space-y-2">
          <textarea
            value={notes}
            onChange={e => setNotes(e.target.value)}
            placeholder="Optional: what was wrong or right?"
            className="w-full border rounded p-2 text-sm"
            rows={2}
          />
          <button onClick={handleSubmit}
            className="px-3 py-1.5 bg-blue-600 text-white rounded text-sm">
            Submit
          </button>
        </div>
      )}
    </div>
  );
}
```
