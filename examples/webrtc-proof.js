// Explicit browser acceptance over the real setup service, never a gameplay relay.
// Run from a page whose origin the chosen setup deployment permits.
async function runWebRtcProof({ lobbyId, relayOnly = false, scoped = false }) {
  const assert = (condition, message) => { if (!condition) throw new Error(message); };
  const deadline = (promise, ms = 20000) => Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("probe timed out")), ms)),
  ]);
  const post = async (path, body, token) => {
    const response = await fetch(path, { method: "POST", headers: {
      "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}),
    }, body: JSON.stringify(body) });
    assert(response.ok, "setup request refused");
    return response.json();
  };
  const queue = (target, maximum = 128) => {
    const items = [];
    const readers = [];
    let failed = false;
    target.addEventListener("message", event => {
      if (items.length >= maximum) { failed = true; return; }
      const next = readers.shift();
      if (next) next(event.data); else items.push(event.data);
    });
    return () => {
      assert(!failed, "probe queue overflowed");
      return deadline(items.length ? Promise.resolve(items.shift()) : new Promise(resolve => readers.push(resolve)));
    };
  };
  const command = sequence => {
    const bytes = new Uint8Array(10);
    bytes.set([3, 1]);
    const view = new DataView(bytes.buffer);
    view.setUint32(2, sequence); view.setUint16(6, 2);
    return bytes;
  };
  const participant = await post(`/lobbies/${lobbyId}/join`, {});
  const signaling = new WebSocket(`${location.origin.replace(/^http/, "ws")}/lobbies/${lobbyId}/connect?participantId=${participant.participantId}`,
    ["multiplayer-setup-v1", `cap.${participant.participantToken}`]);
  const nextSignal = queue(signaling);
  await deadline(new Promise((resolve, reject) => {
    signaling.addEventListener("open", resolve, { once: true });
    signaling.addEventListener("error", () => reject(new Error("signaling failed")), { once: true });
  }));
  let current;
  const connect = async reconnect => {
    const turn = await post(`/lobbies/${lobbyId}/turn-credentials`, { participantId: participant.participantId }, participant.participantToken);
    const stun = turn.iceServers.flatMap(server => server.urls).filter(url => url.startsWith("turn:") && !url.endsWith("transport=tcp"))
      .map(url => `stun:${url.slice(5).split("?")[0]}`);
    const pc = new RTCPeerConnection({ iceServers: [...turn.iceServers, { urls: stun }], iceTransportPolicy: relayOnly ? "relay" : "all" });
    const realtime = pc.createDataChannel("game-server.realtime.v1", { negotiated: true, id: 0, ordered: false, maxRetransmits: 0 });
    const session = pc.createDataChannel("game-server.session.v1", { negotiated: true, id: 1 });
    const commands = pc.createDataChannel("game-server.commands.v1", { negotiated: true, id: 2 });
    for (const channel of [realtime, session, commands]) channel.binaryType = "arraybuffer";
    const nextSnapshot = queue(realtime), nextSession = queue(session), nextAck = queue(commands);
    await pc.setLocalDescription(await pc.createOffer());
    if (pc.iceGatheringState !== "complete") await deadline(new Promise(resolve => {
      pc.addEventListener("icegatheringstatechange", () => { if (pc.iceGatheringState === "complete") resolve(); });
    }));
    signaling.send(JSON.stringify({ type: "signal", to: participant.hostParticipantId,
      payload: { adapter: "game-server-webrtc-v1", kind: "offer", matchId: "webrtc-proof", description: pc.localDescription } }));
    let ticket;
    for (;;) {
      const event = JSON.parse(await nextSignal());
      if (event.type !== "signal" || event.from !== participant.hostParticipantId || event.payload.adapter !== "game-server-webrtc-v1" || event.payload.kind !== "answer") continue;
      ticket = event.payload.ticket;
      assert(Array.isArray(ticket) && ticket.length === 32, "invalid admission ticket");
      await pc.setRemoteDescription(event.payload.description);
      break;
    }
    await deadline(Promise.all([session, realtime, commands].map(channel => channel.readyState === "open" ? Promise.resolve() : new Promise(resolve => channel.addEventListener("open", resolve, { once: true })))));
    const ready = new Uint8Array(await nextSession());
    assert(ready.length === 5 && ready.every((byte, index) => byte === [71,83,65,68,1][index]), "invalid admission readiness marker");
    session.send(new Uint8Array([...ticket, ...(reconnect || [])]));
    const welcome = new Uint8Array(await nextSession());
    assert(welcome.length === 46 && welcome[0] === 3 && welcome[1] === 3, "invalid welcome");
    commands.send(new Uint8Array([71, 83, 67, 76, 1, 3]));
    return { pc, realtime, session, commands, nextSnapshot, nextSession, nextAck, welcome };
  };
  const started = performance.now();
  try {
    current = await connect();
    const setupMs = performance.now() - started;
    const playerId = new DataView(current.welcome.buffer).getUint32(2);
    const firstEpoch = new DataView(current.welcome.buffer).getUint32(18);
    const sendCommand = async (sequence, outcome) => {
      current.commands.send(command(sequence));
      const ack = new Uint8Array(await current.nextAck());
      assert(ack.length === 7 && ack[0] === 3 && ack[1] === 6 && new DataView(ack.buffer).getUint32(2) === sequence && ack[6] === outcome, "wrong reliable acknowledgement");
    };
    for (let i = 0; i < 3; i++) {
      current.realtime.send(command(1));
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    for (;;) {
      const frame = new Uint8Array(await current.nextSnapshot());
      assert(frame[0] === 3 && frame[1] === 2, "invalid intent snapshot");
      const candidate = frame.slice(20), view = new DataView(candidate.buffer);
      let delivered = false;
      for (let offset = 1; offset < candidate.length; offset += 12) {
        if (view.getUint32(offset) === playerId && view.getUint32(offset + 8) === 1) delivered = true;
      }
      if (delivered) break;
    }
    await sendCommand(1, 1); await sendCommand(1, 2);
    const old = current;
    const reconnectStart = performance.now();
    old.pc.close();
    current = await connect(Array.from(old.welcome.slice(22, 38)));
    const reconnectMs = performance.now() - reconnectStart;
    const connectionEpoch = new DataView(current.welcome.buffer).getUint32(18);
    assert(new DataView(current.welcome.buffer).getUint32(2) === playerId && connectionEpoch > firstEpoch, "reconnect fencing failed");
    await sendCommand(1, 2); await sendCommand(2, 1);
    const rtts = [];
    for (let i = 0; i < 10; i++) {
      const start = performance.now();
      current.session.send(new Uint8Array([71,83,67,84,1,1,0,1,42]));
      const response = new Uint8Array(await current.nextSession());
      assert(response.length === 9 && response[5] === 2 && response[8] === 42, "control exchange failed");
      rtts.push(performance.now() - start);
    }
    const ticks = [];
    let payload;
    for (let i = 0; i < 20; i++) {
      const frame = new Uint8Array(await current.nextSnapshot());
      assert(frame[0] === 3 && frame[1] === 2 && frame.length >= 21, "invalid snapshot");
      const view = new DataView(frame.buffer);
      assert(frame.length === 20 + view.getUint16(18), "snapshot length mismatch");
      const tick = view.getBigUint64(2);
      const candidate = frame.slice(20);
      let hash = 0xcbf29ce484222325n;
      const update = byte => { hash = BigInt.asUintN(64, (hash ^ BigInt(byte)) * 0x100000001b3n); };
      frame.slice(2, 10).forEach(update);
      const size = new Uint8Array(8); new DataView(size.buffer).setBigUint64(0, BigInt(candidate.length)); size.forEach(update); candidate.forEach(update);
      assert(hash === view.getBigUint64(10), "snapshot hash mismatch");
      if (ticks.length && tick <= BigInt(ticks.at(-1))) continue;
      ticks.push(Number(tick));
      if (scoped) assert(candidate[0] === 1 && new DataView(candidate.buffer).getUint32(1) === playerId, "private projection disclosed another player");
      for (let offset = 1; offset < candidate.length; offset += 12) {
        if (new DataView(candidate.buffer).getUint32(offset) === playerId && new DataView(candidate.buffer).getUint32(offset + 8) === 2) payload = Array.from(candidate);
      }
    }
    assert(payload, "intent/state failed to converge");
    const report = await current.pc.getStats();
    const transport = [...report.values()].find(stat => stat.type === "transport" && stat.selectedCandidatePairId);
    const pair = transport ? report.get(transport.selectedCandidatePairId) : [...report.values()].find(stat => stat.type === "candidate-pair" && stat.nominated && stat.state === "succeeded");
    assert(pair, "selected ICE pair unavailable");
    const local = report.get(pair.localCandidateId), remote = report.get(pair.remoteCandidateId);
    const relayed = local.candidateType === "relay" || remote.candidateType === "relay";
    if (relayOnly) assert(relayed, "forced TURN selected a direct route");
    const missingSnapshotTicks = ticks.at(-1) - ticks[0] + 1 - ticks.length;
    return { browser: navigator.userAgent, setupMs, reconnectMs, playerId, connectionEpoch, scoped, relayed,
      selectedCandidateTypes: [local.candidateType, remote.candidateType], iceRttMs: pair.currentRoundTripTime * 1000,
      bytesSent: pair.bytesSent, bytesReceived: pair.bytesReceived, controlRttSamplesMs: rtts,
      rttMeanAbsoluteDeltaMs: rtts.slice(1).reduce((sum, value, index) => sum + Math.abs(value - rtts[index]), 0) / 9,
      receivedSnapshotTicks: ticks.length, missingSnapshotTicks, semanticPayload: payload, orderedCommands: true, fencedReconnect: true };
  } finally {
    current?.pc.close(); signaling.close();
  }
}
