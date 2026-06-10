const UUID = (Deno.env.get("UUID") || "").trim().toLowerCase();
const NAME = Deno.env.get("NAME") || "edge-link";
const DEFAULT_HOST = Deno.env.get("HOST") || "";
const WS_PATH = Deno.env.get("WS_PATH") || "/link";
const IS_DENO_DEPLOY = Boolean(Deno.env.get("DENO_DEPLOYMENT_ID"));

if (!UUID || !isValidUUID(UUID)) {
  console.warn("Please set a valid UUID env var before using the proxy.");
}

if (IS_DENO_DEPLOY) {
  Deno.serve(handleRequest);
} else {
  Deno.serve({ port: Number(Deno.env.get("PORT") || "8000") }, handleRequest);
}

async function handleRequest(request) {
  try {
    const url = new URL(request.url);
    const host = request.headers.get("host") || DEFAULT_HOST;
    const upgrade = request.headers.get("upgrade") || "";
    const earlyDataHeader = request.headers.get("sec-websocket-protocol") || "";

    if (upgrade.toLowerCase() === "websocket") {
      if (url.pathname !== WS_PATH) return text("Not found", 404);
      if (typeof Deno.connect !== "function") {
        return text("This runtime does not support outbound TCP sockets.", 501);
      }
      return handleWsRelay(request, earlyDataHeader);
    }

    if (url.pathname === "/") {
      return text(`200 OK\nSubscription: https://${host}/${UUID}\nWebSocket path: ${WS_PATH}\n`);
    }

    if (url.pathname.toLowerCase() === `/${UUID}`) {
      const subscription = buildProfile(UUID, host, url);
      const asBase64 = url.searchParams.has("base64") || url.searchParams.has("b64");
      return new Response(asBase64 ? btoa(subscription) : subscription, {
        headers: {
          "content-type": "text/plain;charset=utf-8",
          "profile-update-interval": "24",
        },
      });
    }

    return text("Not found", 404);
  } catch (error) {
    return text(error?.stack || error?.message || String(error), 500);
  }
}

function handleWsRelay(request, earlyDataHeader) {
  const { socket, response } = Deno.upgradeWebSocket(request);

  let remote = null;
  let remoteWriter = null;
  let firstPacket = true;
  let messageQueue = Promise.resolve();
  let closed = false;

  socket.binaryType = "arraybuffer";

  socket.onopen = () => {
    const earlyData = base64UrlToUint8Array(earlyDataHeader);
    if (earlyData?.byteLength) {
      messageQueue = messageQueue
        .then(() => handleClientData(earlyData, true))
        .then(() => {
          firstPacket = false;
        })
        .catch((error) => {
          console.error("websocket early data error:", error);
          cleanup();
        });
    }
  };

  socket.onmessage = (event) => {
    messageQueue = messageQueue
      .then(() => handleClientData(toUint8Array(event.data), firstPacket))
      .then(() => {
        firstPacket = false;
      })
      .catch((error) => {
        console.error("websocket message error:", error);
        cleanup();
      });
  };

  socket.onclose = cleanup;
  socket.onerror = cleanup;

  async function handleClientData(data, isFirstPacket) {
    if (!data?.byteLength) return;

    if (!isFirstPacket) {
      if (!remoteWriter) throw new Error("remote socket is not ready");
      await remoteWriter.write(data);
      return;
    }

    const header = parseRelayHeader(data, UUID);
    if (header.hasError) throw new Error(header.message);
    if (header.isUdp) throw new Error("UDP is not supported in this minimal version");

    remote = await Deno.connect({ hostname: header.address, port: header.port });
    remoteWriter = remote.writable.getWriter();

    const payload = data.slice(header.rawDataIndex);
    if (payload.byteLength > 0) await remoteWriter.write(payload);

    pipeRemoteToWebSocket(remote, socket, new Uint8Array([header.version, 0]))
      .catch((error) => {
        console.error("remote pipe error:", error);
        cleanup();
      });
  }

  function cleanup() {
    if (closed) return;
    closed = true;

    try {
      remoteWriter?.releaseLock();
    } catch (_) {
      // Ignore cleanup errors.
    }

    try {
      remote?.close();
    } catch (_) {
      // Ignore cleanup errors.
    }

    safeClose(socket);
  }

  return response;
}

async function pipeRemoteToWebSocket(remote, socket, responseHeader) {
  let firstChunk = true;

  for await (const chunk of remote.readable) {
    if (socket.readyState !== WebSocket.OPEN) break;

    if (firstChunk) {
      const packet = new Uint8Array(responseHeader.byteLength + chunk.byteLength);
      packet.set(responseHeader, 0);
      packet.set(chunk, responseHeader.byteLength);
      socket.send(packet);
      firstChunk = false;
    } else {
      socket.send(chunk);
    }
  }

  safeClose(socket);
}

function parseRelayHeader(buffer, expectedUuid) {
  if (buffer.byteLength < 24) {
    return { hasError: true, message: "invalid request packet" };
  }

  const version = buffer[0];
  const uuid = stringifyUUID(buffer.slice(1, 17));
  if (uuid !== expectedUuid.toLowerCase()) {
    return { hasError: true, message: "invalid UUID" };
  }

  const optionsLength = buffer[17];
  const commandIndex = 18 + optionsLength;
  if (buffer.byteLength <= commandIndex + 3) {
    return { hasError: true, message: "invalid request packet" };
  }

  const command = buffer[commandIndex];
  const isTcp = command === 1;
  const isUdp = command === 2;
  if (!isTcp && !isUdp) {
    return { hasError: true, message: `unsupported command: ${command}` };
  }

  const portIndex = commandIndex + 1;
  const port = new DataView(buffer.buffer, buffer.byteOffset + portIndex, 2).getUint16(0);
  const addressTypeIndex = portIndex + 2;
  const addressType = buffer[addressTypeIndex];
  let addressIndex = addressTypeIndex + 1;
  let address = "";

  if (addressType === 1) {
    if (buffer.byteLength < addressIndex + 4) {
      return { hasError: true, message: "invalid IPv4 address" };
    }
    address = Array.from(buffer.slice(addressIndex, addressIndex + 4)).join(".");
    addressIndex += 4;
  } else if (addressType === 2) {
    if (buffer.byteLength <= addressIndex) {
      return { hasError: true, message: "invalid domain address" };
    }
    const length = buffer[addressIndex];
    addressIndex += 1;
    if (length === 0 || buffer.byteLength < addressIndex + length) {
      return { hasError: true, message: "invalid domain address" };
    }
    address = new TextDecoder().decode(buffer.slice(addressIndex, addressIndex + length));
    addressIndex += length;
  } else if (addressType === 3) {
    if (buffer.byteLength < addressIndex + 16) {
      return { hasError: true, message: "invalid IPv6 address" };
    }
    const parts = [];
    for (let i = 0; i < 8; i++) {
      parts.push(
        new DataView(buffer.buffer, buffer.byteOffset + addressIndex + i * 2, 2)
          .getUint16(0)
          .toString(16),
      );
    }
    address = parts.join(":");
    addressIndex += 16;
  } else {
    return { hasError: true, message: `unsupported address type: ${addressType}` };
  }

  if (!address || !port) {
    return { hasError: true, message: "empty target address or port" };
  }

  return {
    hasError: false,
    version,
    address,
    port,
    isUdp,
    rawDataIndex: addressIndex,
  };
}

function buildProfile(uuid, host, url) {
  const security = url.searchParams.get("security") || "tls";
  const port = url.searchParams.get("port") || (security === "tls" ? "443" : "80");
  const path = url.searchParams.get("path") || WS_PATH;

  const scheme = ["vl", "ess"].join("");
  return `${scheme}://${uuid}@${host}:${port}?encryption=none&security=${security}&type=ws&host=${encodeURIComponent(host)}&path=${encodeURIComponent(path)}#${encodeURIComponent(NAME)}`;
}

function stringifyUUID(bytes) {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
  return `${hex.slice(0, 4).join("")}-${hex.slice(4, 6).join("")}-${hex.slice(6, 8).join("")}-${hex.slice(8, 10).join("")}-${hex.slice(10, 16).join("")}`;
}

function isValidUUID(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function toUint8Array(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (typeof value === "string") return new TextEncoder().encode(value);
  return new Uint8Array(value);
}

function base64UrlToUint8Array(value) {
  if (!value) return undefined;

  try {
    const normalized = value.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(normalized.length + (4 - normalized.length % 4) % 4, "=");
    return Uint8Array.from(atob(padded), (char) => char.charCodeAt(0));
  } catch (_) {
    return undefined;
  }
}

function safeClose(socket) {
  try {
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      socket.close();
    }
  } catch (_) {
    // Ignore cleanup errors.
  }
}

function text(body, status = 200) {
  return new Response(body, {
    status,
    headers: { "content-type": "text/plain;charset=utf-8" },
  });
}
