(function () {
  // EventEmitter class based on the Emitter in starx-wsclient.js
  function Emitter(obj) {
    if (obj) return mixin(obj);
  }

  function mixin(obj) {
    for (var key in Emitter.prototype) {
      obj[key] = Emitter.prototype[key];
    }
    return obj;
  }

  Emitter.prototype.on = Emitter.prototype.addEventListener = function (
    event,
    fn
  ) {
    this._callbacks = this._callbacks || {};
    (this._callbacks[event] = this._callbacks[event] || []).push(fn);
    return this;
  };

  Emitter.prototype.once = function (event, fn) {
    var self = this;
    this._callbacks = this._callbacks || {};

    function on() {
      self.off(event, on);
      fn.apply(this, arguments);
    }

    on.fn = fn;
    this.on(event, on);
    return this;
  };

  Emitter.prototype.off =
    Emitter.prototype.removeListener =
    Emitter.prototype.removeAllListeners =
    Emitter.prototype.removeEventListener =
      function (event, fn) {
        this._callbacks = this._callbacks || {};

        // Remove all handlers
        if (0 == arguments.length) {
          this._callbacks = {};
          return this;
        }

        var callbacks = this._callbacks[event];
        if (!callbacks) return this;

        // Remove all handlers for specific event
        if (1 == arguments.length) {
          delete this._callbacks[event];
          return this;
        }

        // Remove specific handler
        var cb;
        for (var i = 0; i < callbacks.length; i++) {
          cb = callbacks[i];
          if (cb === fn || cb.fn === fn) {
            callbacks.splice(i, 1);
            break;
          }
        }
        return this;
      };

  Emitter.prototype.emit = function (event) {
    this._callbacks = this._callbacks || {};
    var args = [].slice.call(arguments, 1),
      callbacks = this._callbacks[event];

    if (callbacks) {
      callbacks = callbacks.slice(0);
      for (var i = 0, len = callbacks.length; i < len; ++i) {
        callbacks[i].apply(this, args);
      }
    }

    return this;
  };

  Emitter.prototype.listeners = function (event) {
    this._callbacks = this._callbacks || {};
    return this._callbacks[event] || [];
  };

  Emitter.prototype.hasListeners = function (event) {
    return !!this.listeners(event).length;
  };

  var root = window;
  var nano = Object.create(Emitter.prototype);
  root.nano = nano;

  var sseConnected = false;
  var eventSource = null;
  var baseUrl = "";

  nano.init = function (params, cb) {
    baseUrl = params.baseUrl || "";
    // Initialize SSE connection
    nano.connectSSE(params.ssePath || "/sse", cb);
  };

  nano.connectSSE = function (ssePath, cb) {
    if (sseConnected) {
      return;
    }
    eventSource = new EventSource(baseUrl + ssePath);

    eventSource.onopen = function (...arg) {
      console.log("SSE connected, args:", arg);
      sseConnected = true;
      nano.emit("connect");
      if (cb) {
        cb();
      }
    };

    eventSource.onmessage = function (event) {
      var msg = JSON.parse(event.data);
      console.log("SSE message:", msg);

      // Check if sse_sessionID exists
      if (msg.body && msg.body.sse_sessionID) {
        console.log("SSE message:", msg.body.sse_sessionID);
        // Store sse_sessionID in a cookie
        document.cookie =
          "sse_sessionID=" + msg.body.sse_sessionID + "; path=/";
        nano.sse_sessionID = msg.body.sse_sessionID;
      }

      if (msg.route) {
        nano.emit(msg.route, msg.body);
      } else {
        // Default event handler
        nano.emit("message", msg);
      }
    };

    eventSource.onerror = function (err) {
      nano.emit("error", err);
      console.error("SSE error:", err);
      sseConnected = false;
      eventSource.close();

      // Reconnect after a delay
      setTimeout(function () {
        console.log("Attempting to reconnect SSE...");
        nano.connectSSE(ssePath, cb);
      }, 5000); // Retry after 5 seconds
    };
  };

  // Helper function to retrieve a cookie by name
  function getCookie(name) {
    var value = "; " + document.cookie;
    var parts = value.split("; " + name + "=");
    if (parts.length === 2) return parts.pop().split(";").shift();
    return null;
  }

  nano.request = function (route, msg, cb) {
    if (arguments.length === 2 && typeof msg === "function") {
      cb = msg;
      msg = {};
    } else {
      msg = msg || {};
    }
    route = route || msg.route;
    if (!route) {
      return;
    }

    // Make an HTTP POST request to the API endpoint
    fetch(baseUrl + "/api", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        // Include sse_sessionID in headers
        "X-SSE-SessionID": nano.sse_sessionID || "",
      },
      body: JSON.stringify({
        route: route,
        data: msg,
      }),
    })
      .then(function (response) {
        return response.json();
      })
      .then(function (data) {
        if (cb) cb(data);
      })
      .catch(function (error) {
        console.error("Error in request:", error);
        if (cb) cb({ code: 500, error: error });
      });
  };

  nano.notify = function (route, msg) {
    msg = msg || {};
    route = route || msg.route;

    // Make an HTTP POST request without expecting a response
    fetch(baseUrl + "/api", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-SSE-SessionID": nano.sse_sessionID || "",
      },
      body: JSON.stringify({
        type: 1,
        route: route,
        data: msg,
      }),
    }).catch(function (error) {
      console.error("Error in notify:", error);
    });
  };

  nano.disconnect = function () {
    if (eventSource) {
      eventSource.close();
      sseConnected = false;
      eventSource = null;
      nano.emit("disconnect");
    }
  };
})();
