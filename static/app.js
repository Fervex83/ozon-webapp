const config = window.PAGE_CONFIG || {};
const marketplaces = config.marketplaces || [];
const tsList = config.ts_list || [];
const sellerAliases = config.seller_aliases || {};

const marketplaceSelect = document.getElementById("marketplace");
const tsSelect = document.getElementById("tsSelect");
const tsField = document.getElementById("tsField");
const zoneSelect = document.getElementById("zoneSelect");
const phoneField = document.getElementById("phoneField");
const phoneInput = document.getElementById("phoneInput");
const themeToggle = document.getElementById("themeToggle");

const searchInput = document.getElementById("searchInput");
const searchFilter = document.getElementById("searchFilter");
const searchOptions = document.getElementById("searchOptions");
const sellerInput = document.getElementById("sellerInput");
const sellerFilter = document.getElementById("sellerFilter");
const sellerOptions = document.getElementById("sellerOptions");

const errorSelect = document.getElementById("errorSelect");
const okSelect = document.getElementById("okSelect");
const okRuleList = document.getElementById("okRuleList");
const errorRuleList = document.getElementById("errorRuleList");
const errorManualList = document.getElementById("errorManualList");
const okManualList = document.getElementById("okManualList");
const okEnabled = document.getElementById("okEnabled");
const errorEnabled = document.getElementById("errorEnabled");

const batchUrls = document.getElementById("batchUrls");
const runBatchBtn = document.getElementById("runBatchBtn");
const resumeBatchBtn = document.getElementById("resumeBatchBtn");
const runSearchBtn = document.getElementById("runSearchBtn");
const stopBtn = document.getElementById("stopBtn");
const queueInfo = document.getElementById("queueInfo");
const searchInfo = document.getElementById("searchInfo");
const phaseInfo = document.getElementById("phaseInfo");
const etaInfo = document.getElementById("etaInfo");
const collectInfo = document.getElementById("collectInfo");
const phaseCounts = document.getElementById("phaseCounts");
const checkedCounts = document.getElementById("checkedCounts");
const xlsxLink = document.getElementById("xlsxLink");
const searchXlsxLinkTop = document.getElementById("searchXlsxLinkTop");
const pendingList = document.getElementById("pendingList");
const resultList = document.getElementById("resultList");
const scenarioTitle = document.getElementById("scenarioTitle");
const resultStatus = document.getElementById("resultStatus");
const resultProgress = document.getElementById("resultProgress");
const searchFreshProfile = document.getElementById("searchFreshProfile");
const freshProfileTip = document.getElementById("freshProfileTip");
const filterOk = document.getElementById("filterOk");
const filterNok = document.getElementById("filterNok");
const filterUnknown = document.getElementById("filterUnknown");
const filterError = document.getElementById("filterError");
const resultStats = document.getElementById("resultStats");
const formLockEls = [
  marketplaceSelect,
  tsSelect,
  zoneSelect,
  phoneInput,
  searchInput,
  searchFilter,
  sellerInput,
  sellerFilter,
  batchUrls,
  okSelect,
  errorSelect,
  okEnabled,
  errorEnabled,
  searchFreshProfile,
];

let currentTsId = config.default_ts_id || "";
let presets = config.presets || {};
let pollingTimer = null;
let currentJobId = null;
let currentSearchJobId = null;
let jobStartedAt = null;
let lastDoneCount = 0;
let lastDoneTs = null;
let etaSamples = [];
let lastSearchUrls = [];
let lastResults = [];

const setTheme = (theme) => {
  document.body.dataset.theme = theme;
  if (themeToggle) {
    themeToggle.checked = theme === "light";
  }
  try {
    localStorage.setItem("ozonTheme", theme);
  } catch (err) {
    // ignore storage errors
  }
};

if (freshProfileTip) {
  freshProfileTip.title = "Для удаления cookies";
}



const loadTheme = () => {
  let stored = null;
  try {
    stored = localStorage.getItem("ozonTheme");
  } catch (err) {
    stored = null;
  }
  setTheme(stored || "dark");
};

const setMarketplaceTheme = (marketplaceId) => {
  document.body.dataset.marketplace = marketplaceId || "ozon";
};

const populateMarketplaceSelect = () => {
  marketplaceSelect.textContent = "";
  marketplaces.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.id;
    option.textContent = item.name;
    option.disabled = !item.enabled;
    marketplaceSelect.appendChild(option);
  });
  const active = marketplaces.find((item) => item.enabled);
  if (active) {
    marketplaceSelect.value = active.id;
    setMarketplaceTheme(active.id);
  }
};

const populateTsSelect = (marketplaceId) => {
  tsSelect.textContent = "";
  const filtered = tsList.filter((ts) => ts.marketplace === marketplaceId);
  filtered.forEach((ts) => {
    const option = document.createElement("option");
    option.value = ts.id;
    option.textContent = ts.name;
    tsSelect.appendChild(option);
  });
  if (filtered.length > 0) {
    if (tsField) {
      tsField.style.display = "block";
    }
    const storedTs = localStorage.getItem("ozonLastTsId");
    const preferred = filtered.find((ts) => ts.id === (storedTs || currentTsId))
      ? (storedTs || currentTsId)
      : filtered[0].id;
    const defaultId = preferred;
    tsSelect.value = defaultId;
    currentTsId = defaultId;
    updateScenarioTitle(defaultId);
  } else {
    currentTsId = "";
    updateScenarioTitle("");
    if (tsField) {
      tsField.style.display = "none";
    }
  }
};

const updateScenarioTitle = (tsId) => {
  if (!scenarioTitle) return;
  const match = tsList.find((ts) => ts.id === tsId);
  scenarioTitle.textContent = match ? match.name : "—";
};

const renderOptionList = (options, filterValue, target, onPick) => {
  target.textContent = "";
  const normalized = (filterValue || "").toLowerCase();
  const filtered = options.filter((item) => item.toLowerCase().includes(normalized));
  filtered.forEach((optionValue) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "option-pill";
    button.textContent = optionValue;
    if (optionValue.toLowerCase() === "ozon") {
      const key = Object.keys(sellerAliases).find((k) => k.toLowerCase() === "ozon");
      const values = key ? sellerAliases[key] : null;
      if (values && Array.isArray(values) && values.length > 0) {
        const tip = document.createElement("span");
        tip.className = "help-tip";
        tip.textContent = "?";
        tip.title = `Включает: ${values.join(", ")}`;
        button.appendChild(tip);
      }
    }
    button.addEventListener("click", (event) => onPick(optionValue, event));
    target.appendChild(button);
  });
  if (filtered.length === 0) {
    const empty = document.createElement("div");
    empty.className = "option-empty";
    empty.textContent = "Нет совпадений";
    target.appendChild(empty);
  }
  return filtered;
};

const splitSellerValues = (value) => {
  if (!value) return [];
  return value
    .split(/[,;\n]+/)
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
};

const appendSellerValue = (inputEl, value) => {
  const incoming = value.trim();
  if (!incoming) return;
  const existing = splitSellerValues(inputEl.value);
  const incomingNorm = incoming.toLowerCase();
  const hasDuplicate = existing.some((item) => item.toLowerCase() === incomingNorm);
  if (hasDuplicate) return;
  existing.push(incoming);
  inputEl.value = existing.join("; ");
};

const setMode = (field, mode) => {
  const block = document.querySelector(`.field-block[data-field="${field}"]`);
  if (!block) return;
  const input = field === "search" ? searchInput : sellerInput;
  const panel = block.querySelector(".preset-panel");
  const buttons = block.querySelectorAll(".mode-btn");
  buttons.forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.mode === mode);
  });

  if (mode === "manual") {
    input.readOnly = false;
    panel.classList.remove("active");
  } else {
    input.readOnly = true;
    panel.classList.add("active");
  }
};

const initModeSwitches = () => {
  document.querySelectorAll(".mode-switch").forEach((switcher) => {
    const field = switcher.dataset.target;
    switcher.addEventListener("click", (event) => {
      const btn = event.target.closest(".mode-btn");
      if (!btn) return;
      setMode(field, btn.dataset.mode);
    });
  });
};

const fillSelectOptions = (selectEl, values) => {
  selectEl.textContent = "";
  values.forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = `${value} ★`;
    option.dataset.preset = "1";
    selectEl.appendChild(option);
  });
};

const renderRuleCheckboxes = (listEl, values, presetSet) => {
  if (!listEl) return;
  listEl.textContent = "";
  values.forEach((value) => {
    const row = document.createElement("label");
    row.className = "rule-item";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = value;
    const text = document.createElement("span");
    text.textContent = value;
    row.appendChild(checkbox);
    row.appendChild(text);
    const isPreset = presetSet && presetSet.has(value);
    if (!isPreset) {
      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.textContent = "×";
      removeBtn.className = "rule-remove";
      removeBtn.addEventListener("click", (event) => {
        event.preventDefault();
        row.remove();
        const opt = Array.from(
          (listEl.id === "okRuleList" ? okSelect : errorSelect).options
        ).find((o) => o.value === value);
        if (opt) opt.remove();
      });
      row.appendChild(removeBtn);
    }
    listEl.appendChild(row);
  });
};

const getCheckedRuleValues = (listEl) => {
  if (!listEl) return [];
  return Array.from(listEl.querySelectorAll("input[type='checkbox']:checked")).map(
    (el) => el.value
  );
};

const updatePresets = (data) => {
  presets = data || {};
  const searchValues = presets.search_phrases || [];
  const sellerValues = presets.sellers || [];
  renderOptionList(searchValues, searchFilter.value, searchOptions, (value) => {
    searchInput.value = value;
  });
  renderOptionList(sellerValues, sellerFilter.value, sellerOptions, (value, event) => {
    if (event && (event.ctrlKey || event.metaKey)) {
      appendSellerValue(sellerInput, value);
    } else {
      sellerInput.value = value;
    }
  });
  fillSelectOptions(errorSelect, presets.error_texts || []);
  fillSelectOptions(okSelect, presets.ok_texts || []);
  const errorPresetSet = new Set(presets.error_texts || []);
  const okPresetSet = new Set(presets.ok_texts || []);
  renderRuleCheckboxes(errorRuleList, presets.error_texts || [], errorPresetSet);
  renderRuleCheckboxes(okRuleList, presets.ok_texts || [], okPresetSet);

  if (searchValues.length > 0) {
    searchInput.value = searchValues[0];
  }
  if (sellerValues.length > 0) {
    sellerInput.value = sellerValues[0];
  }

  if (currentTsId === "ozon_tecno" && okRuleList) {
    okRuleList.querySelectorAll("input[type='checkbox']").forEach((el) => {
      el.checked = true;
    });
  }
};

const fetchPresets = async (tsId) => {
  if (!tsId) {
    updatePresets({});
    return;
  }
  try {
    const res = await fetch(`/api/presets/${tsId}`);
    const data = await res.json();
    if (res.ok && data.ok) {
      updatePresets(data.presets || {});
    }
  } catch (err) {
    updatePresets({});
  }
};

const addManualRule = (listEl, selectEl, value) => {
  const trimmed = value.trim();
  if (!trimmed) return;
  const existing = Array.from(selectEl.options).find(
    (opt) => opt.value.trim().toLowerCase() === trimmed.toLowerCase()
  );
  let option = existing;
  if (!option) {
    option = document.createElement("option");
    option.value = trimmed;
    option.textContent = trimmed;
    selectEl.appendChild(option);
  }
  option.selected = true;

  const chip = document.createElement("div");
  chip.className = "rule-chip";
  const textNode = document.createTextNode(trimmed);
  chip.appendChild(textNode);
  const removeBtn = document.createElement("button");
  removeBtn.type = "button";
  removeBtn.textContent = "×";
  removeBtn.addEventListener("click", () => {
    chip.remove();
    if (option && option.parentNode) {
      option.remove();
    }
  });
  chip.appendChild(removeBtn);
  listEl.appendChild(chip);

  if (selectEl === okSelect && okRuleList) {
    renderRuleCheckboxes(
      okRuleList,
      Array.from(okSelect.options).map((opt) => opt.value),
      new Set(presets.ok_texts || [])
    );
  }
  if (selectEl === errorSelect && errorRuleList) {
    renderRuleCheckboxes(
      errorRuleList,
      Array.from(errorSelect.options).map((opt) => opt.value),
      new Set(presets.error_texts || [])
    );
  }
};

const initRuleBlocks = () => {
  document.querySelectorAll(".rule-card").forEach((card) => {
    const listEl = card.querySelector(".rule-list");
    const rowInput = card.querySelector(".rule-row input");
    const addBtn = card.querySelector(".add-rule");
    const selectEl = card.querySelector("select");
    const pickerEl = card.querySelector(".emoji-picker");
    addBtn.addEventListener("click", () => {
      addManualRule(listEl, selectEl, rowInput.value);
      rowInput.value = "";
    });
    rowInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        addManualRule(listEl, selectEl, rowInput.value);
        rowInput.value = "";
      }
    });
    card.querySelectorAll(".emoji-trigger").forEach((btn) => {
      btn.addEventListener("click", () => {
        rowInput.focus();
        if (pickerEl) {
          pickerEl.classList.toggle("active");
        }
      });
    });
    if (pickerEl) {
      const emojis = Array.isArray(window.EMOJI_LIST) && window.EMOJI_LIST.length > 0
        ? window.EMOJI_LIST
        : ["🎁"];
      pickerEl.textContent = "";
      emojis.forEach((emoji) => {
        const button = document.createElement("button");
        button.type = "button";
        button.textContent = emoji;
        button.addEventListener("click", () => {
          rowInput.value = `${rowInput.value}${emoji}`;
          rowInput.focus();
        });
        pickerEl.appendChild(button);
      });
    }
  });
};

const collectManualRules = (listEl) => {
  return Array.from(listEl.querySelectorAll(".rule-chip")).map((chip) =>
    chip.firstChild.textContent.trim()
  );
};

const collectRules = () => {
  const errorValues = errorEnabled.checked ? getCheckedRuleValues(errorRuleList) : [];
  const okValues = okEnabled.checked ? getCheckedRuleValues(okRuleList) : [];
  return {
    error_conditions: [
      ...errorValues,
      ...(errorEnabled.checked ? collectManualRules(errorManualList) : []),
    ],
    ok_conditions: [...okValues, ...(okEnabled.checked ? collectManualRules(okManualList) : [])],
  };
};

const updateQueueInfo = (text) => {
  queueInfo.textContent = text;
};

const setFormLocked = (locked) => {
  formLockEls.forEach((el) => {
    if (!el) return;
    el.disabled = Boolean(locked);
  });
  document.querySelectorAll(".mode-switch .mode-btn").forEach((btn) => {
    btn.disabled = Boolean(locked);
  });
  document.querySelectorAll(".clear-btn, .clear-select, .add-rule, .emoji-trigger").forEach(
    (btn) => {
      btn.disabled = Boolean(locked);
    }
  );
  if (runSearchBtn) {
    runSearchBtn.disabled = Boolean(locked);
  }
  if (runBatchBtn) {
    runBatchBtn.disabled = Boolean(locked);
  }
  if (stopBtn && locked) {
    stopBtn.disabled = false;
  }
};

const setResumeVisible = (visible) => {
  if (!resumeBatchBtn) return;
  resumeBatchBtn.style.display = visible ? "inline-flex" : "none";
};

const tryRestoreJob = async () => {
  const lastJobId = localStorage.getItem("ozonLastJobId");
  if (!lastJobId) return;
  try {
    const res = await fetch(`/jobs/${lastJobId}`);
    const data = await res.json();
    if (!res.ok || !data.ok) return;
    if (data.status === "running" || data.status === "queued") {
      currentJobId = lastJobId;
      pollJob(lastJobId);
      setResumeVisible(false);
      return;
    }
    if (data.status === "stopped") {
      setResumeVisible(true);
    }
  } catch (err) {
    // ignore
  }
};

const getActiveVerdictFilters = () => {
  const out = [];
  if (filterOk && filterOk.checked) out.push("ok");
  if (filterNok && filterNok.checked) out.push("nok");
  if (filterUnknown && filterUnknown.checked) out.push("unknown");
  if (filterError && filterError.checked) out.push("error");
  return out;
};

const renderResults = (results, currentUrl) => {
  if (pendingList) {
    pendingList.textContent = "";
  }
  resultList.textContent = "";
  if (currentUrl) {
    const item = document.createElement("div");
    item.className = "result-item is-current";
    item.innerHTML = `<span class="result-label">Текущий тест</span><a href="${currentUrl}" target="_blank" rel="noopener">${currentUrl}</a>`;
    if (pendingList) {
      pendingList.appendChild(item);
    }
  }
  if (!results || results.length === 0) {
    const empty = document.createElement("div");
    empty.className = "result-empty";
    empty.textContent = "Пока нет результатов.";
    resultList.appendChild(empty);
  }
  const filters = getActiveVerdictFilters();
  results.forEach((item) => {
    const row = document.createElement("div");
    let verdict = item.verdict || "unknown";
    if (!filters.includes(verdict)) {
      return;
    }
    row.className = `result-item verdict-${verdict}`;
    const label =
      verdict === "ok" ? "OK" : verdict === "nok" ? "NOK" : verdict === "error" ? "ERR" : "UNK";
    row.innerHTML = `<span class="result-label">${label}</span><a href="${item.url}" target="_blank" rel="noopener">${item.url}</a><span class="result-note">${item.verdict_reason || ""}</span>`;
    resultList.appendChild(row);
  });
};

const updateResultStats = (results) => {
  if (!resultStats) return;
  const totals = { ok: 0, nok: 0, unknown: 0, error: 0 };
  (results || []).forEach((item) => {
    const verdict = item.verdict || "unknown";
    if (totals.hasOwnProperty(verdict)) {
      totals[verdict] += 1;
    } else {
      totals.unknown += 1;
    }
  });
  const total = totals.ok + totals.nok + totals.unknown + totals.error;
  resultStats.textContent = `Всего/OK/NOK/Другое: ${total}/${totals.ok}/${totals.nok}/${
    totals.unknown + totals.error
  }`;
};

const updateXlsxLinkWithFilter = (jobId) => {
  if (!xlsxLink || !jobId) return;
  const filters = getActiveVerdictFilters();
  const query = filters.length > 0 ? `?verdict=${filters.join(",")}` : "";
  xlsxLink.href = `/jobs/${jobId}/xlsx${query}`;
};

const renderPending = (pendingUrls) => {
  if (!pendingList) return;
  pendingList.textContent = "";
  if (!pendingUrls || pendingUrls.length === 0) {
    const empty = document.createElement("div");
    empty.className = "result-empty";
    empty.textContent = "Нет ожидающих ссылок.";
    pendingList.appendChild(empty);
    return;
  }
  pendingUrls.forEach((url) => {
    const row = document.createElement("div");
    row.className = "result-item";
    row.innerHTML = `<span class="result-label">PENDING</span><a href="${url}" target="_blank" rel="noopener">${url}</a>`;
    pendingList.appendChild(row);
  });
};

const pollJob = async (jobId, options = {}) => {
  const { isSearchOnly = false } = options;
  if (pollingTimer) {
    clearInterval(pollingTimer);
  }

  const tick = async () => {
    try {
      const res = await fetch(`/jobs/${jobId}`);
      const data = await res.json();
      if (!res.ok || !data.ok) {
        updateQueueInfo("Очередь: ошибка получения статуса");
        xlsxLink.classList.remove("active");
        return;
      }
      if (isSearchOnly && searchInfo) {
        searchInfo.textContent = `Поиск: ${data.status} (${data.done}/${data.total})`;
      } else {
        updateQueueInfo(`Очередь: ${data.status} (${data.done}/${data.total})`);
      }
      if (data.error) {
        if (isSearchOnly && searchInfo) {
          searchInfo.textContent = data.error;
        } else {
          updateQueueInfo(data.error);
        }
      }
      if (stopBtn) {
        stopBtn.disabled = data.status !== "running" && data.status !== "queued";
      }
      if (!isSearchOnly) {
        setFormLocked(data.status === "running" || data.status === "queued");
      }
      if (collectInfo) {
        const collected = data.collected_count != null ? data.collected_count : data.total;
        collectInfo.textContent = `Собрано ссылок: ${collected}`;
      }
      if (phaseInfo) {
        const phase = data.phase;
        const phaseCount = typeof data.phase_count === "number" ? data.phase_count : null;
        const countSuffix = phaseCount !== null ? ` (${phaseCount})` : "";
        if (phase === "search") {
          phaseInfo.textContent = `Сбор карточек поиска${countSuffix}`;
          phaseInfo.classList.remove("phase-highlight");
        } else if (phase === "seller") {
          phaseInfo.textContent = `Прохождение ТС${countSuffix}`;
          phaseInfo.classList.remove("phase-highlight");
        } else if (phase === "testing") {
          phaseInfo.textContent = "ТС Идет тестирование";
          phaseInfo.classList.add("phase-highlight");
        } else if (data.search_done && data.status === "running") {
          phaseInfo.textContent = "ТС Идет тестирование";
          phaseInfo.classList.add("phase-highlight");
        } else if (data.search_done) {
          phaseInfo.textContent = "Фаза: сбор завершён";
          phaseInfo.classList.remove("phase-highlight");
        } else {
          phaseInfo.textContent = "Сбор карточек поиска";
          phaseInfo.classList.remove("phase-highlight");
        }
      }
      if (etaInfo) {
        if (jobStartedAt === null && data.started_at) {
          jobStartedAt = data.started_at;
        }
        const elapsedSec = jobStartedAt ? Math.max(Date.now() / 1000 - jobStartedAt, 0) : 0;
        const elapsedMin = elapsedSec ? Math.ceil(elapsedSec / 60) : 0;
        if (data.done > lastDoneCount) {
          const now = Date.now();
          const delta = data.done - lastDoneCount;
          if (lastDoneTs) {
            const elapsedSec = (now - lastDoneTs) / 1000;
            const perItem = elapsedSec / delta;
            for (let i = 0; i < delta; i += 1) {
              etaSamples.push(perItem);
            }
            if (etaSamples.length > 10) {
              etaSamples = etaSamples.slice(-10);
            }
          }
          lastDoneCount = data.done;
          lastDoneTs = now;
        }
        if (data.status === "running" && data.total && data.done) {
          let perItem = null;
          if (etaSamples.length > 0) {
            const sum = etaSamples.reduce((acc, val) => acc + val, 0);
            perItem = sum / etaSamples.length;
          }
          if (!perItem || !Number.isFinite(perItem)) {
            perItem = 12;
          }
          const remaining = Math.max(data.total - data.done, 0);
          const etaSec = Math.round(remaining * perItem);
          etaInfo.textContent = `Ожидаемое время теста: ~${Math.ceil(
            etaSec / 60
          )} мин • Прошло: ${elapsedMin} мин`;
        } else if (data.status === "done") {
          etaInfo.textContent = `Ожидаемое время теста: завершено • Прошло: ${elapsedMin} мин`;
        } else {
          etaInfo.textContent = `Ожидаемое время теста: — • Прошло: ${elapsedMin} мин`;
        }
      }
      if (phaseCounts) {
        if (typeof data.search_total === "number" && data.search_total > 0) {
          const sellerKept = typeof data.seller_kept === "number" ? data.seller_kept : 0;
          if (sellerKept > 0 || data.phase === "seller" || data.search_done) {
            phaseCounts.textContent = `Продавец/Поиск: ${sellerKept}/${data.search_total}`;
          } else {
            phaseCounts.textContent = `Поиск: ${data.search_total}`;
          }
        } else {
          phaseCounts.textContent = "Продавец/Поиск: —";
        }
      }
      if (checkedCounts) {
        if (typeof data.search_total === "number" && data.search_total > 0) {
          const checked =
            typeof data.seller_checked === "number" && data.seller_checked > 0
              ? data.seller_checked
              : data.done || 0;
          checkedCounts.textContent = `Проверено карточек: ${checked}/${data.search_total}`;
        } else {
          checkedCounts.textContent = "Проверено карточек: —";
        }
      }
      lastResults = data.results || [];
      renderPending(data.pending_urls || []);
      renderResults(lastResults, data.current_url);
      updateResultStats(lastResults);

      if (data.search_done) {
        const collected = [];
        (data.pending_urls || []).forEach((url) => collected.push(url));
        (data.results || []).forEach((item) => {
          if (item && item.url) collected.push(item.url);
        });
        if (collected.length > 0) {
          const unique = [];
          const seen = new Set();
          collected.forEach((url) => {
            if (!seen.has(url)) {
              seen.add(url);
              unique.push(url);
            }
          });
          lastSearchUrls = unique;
        }
      }

      if (resultStatus && !isSearchOnly) {
        resultStatus.textContent =
          data.status === "done"
            ? "Done"
            : data.status === "stopped"
            ? "Error"
            : "Running";
        resultStatus.classList.toggle("status-ok", data.status === "done");
        resultStatus.classList.toggle("status-error", data.status === "stopped");
        resultStatus.classList.toggle("status-warn", data.status !== "done" && data.status !== "stopped");
      }
      if (resultProgress && !isSearchOnly) {
        resultProgress.textContent = `${data.done}/${data.total}`;
      }
      if (data.status === "done") {
        clearInterval(pollingTimer);
        pollingTimer = null;
        if (isSearchOnly && searchInfo) {
          searchInfo.textContent = "Поиск: завершено";
        } else {
          updateQueueInfo("Очередь: завершено");
        }
        setFormLocked(false);
        updateXlsxLinkWithFilter(jobId);
        xlsxLink.classList.add("active");
        if (searchXlsxLinkTop) {
          searchXlsxLinkTop.href = `/jobs/${jobId}/search-xlsx`;
          searchXlsxLinkTop.classList.add("active");
        }
        if (stopBtn) {
          stopBtn.disabled = true;
        }
      }
      if (data.status === "stopped") {
        clearInterval(pollingTimer);
        pollingTimer = null;
        if (isSearchOnly && searchInfo) {
          searchInfo.textContent = "Поиск: остановлено";
        } else {
          updateQueueInfo("Очередь: остановлено");
        }
        setFormLocked(false);
        if (stopBtn) {
          stopBtn.disabled = true;
        }
      }
      if (data.search_done && jobId) {
        updateXlsxLinkWithFilter(jobId);
        xlsxLink.classList.add("active");
        if (searchXlsxLinkTop) {
          searchXlsxLinkTop.href = `/jobs/${jobId}/search-xlsx`;
          searchXlsxLinkTop.classList.add("active");
        }
      }
    } catch (err) {
      updateQueueInfo("Очередь: ошибка сети");
      xlsxLink.classList.remove("active");
    }
  };

  await tick();
  pollingTimer = setInterval(tick, 2500);
};

const runBatch = async () => {
  const hasSavedUrls = Array.isArray(lastSearchUrls) && lastSearchUrls.length > 0;
  const searchValue = searchInput.value.trim();
  if (!hasSavedUrls && !searchValue) {
    updateQueueInfo("Очередь: не заполнен поиск");
    return;
  }
  const rules = collectRules();
  const hasRules = rules.error_conditions.length > 0 || rules.ok_conditions.length > 0;
  if ((okEnabled.checked || errorEnabled.checked) && !hasRules) {
    updateQueueInfo("Очередь: правила не выбраны");
    return;
  }
  updateQueueInfo("Очередь: ставлю задачу…");
  xlsxLink.classList.remove("active");
  runBatchBtn.disabled = true;
  try {
    const endpoint = hasSavedUrls ? "/batch" : "/auto-batch";
    const searchSettings = {
      fresh_profile: Boolean(searchFreshProfile?.checked),
    };
    const payload = hasSavedUrls
      ? {
          urls: lastSearchUrls.join("\n"),
        }
      : {
          search: searchValue,
          seller: sellerInput.value.trim(),
          search_settings: searchSettings,
        };
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...payload,
        rules,
        meta: {
          marketplace: marketplaceSelect.value,
          ts_id: tsSelect.value,
          zone: zoneSelect.value,
          phone: phoneInput.value.trim(),
          search: searchInput.value.trim(),
          seller: sellerInput.value.trim(),
        },
      }),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      updateQueueInfo(data.error || "Очередь: ошибка постановки");
      return;
    }
    currentJobId = data.job_id;
    try {
      localStorage.setItem("ozonLastJobId", data.job_id);
    } catch (err) {
      // ignore
    }
    setFormLocked(true);
    jobStartedAt = null;
    lastDoneCount = 0;
    lastDoneTs = null;
    etaSamples = [];
    if (resultStatus) {
      resultStatus.textContent = "Running";
      resultStatus.classList.add("status-warn");
      resultStatus.classList.remove("status-ok", "status-error");
    }
    if (stopBtn) {
      stopBtn.disabled = false;
    }
    updateQueueInfo(`Очередь: задача ${data.job_id} (${data.total})`);
    pollJob(data.job_id);
  } catch (err) {
    updateQueueInfo("Очередь: ошибка сети");
  } finally {
    runBatchBtn.disabled = false;
  }
};

zoneSelect.addEventListener("change", () => {
  if (zoneSelect.value === "auth") {
    phoneField.classList.add("active");
  } else {
    phoneField.classList.remove("active");
  }
});

const autoSearchFromInput = (inputEl, options, filterEl, targetEl, autoPickSingle = true) => {
  const value = inputEl.value.trim();
  if (!value) return;
  filterEl.value = value;
  const filtered = renderOptionList(options, value, targetEl, (picked, event) => {
    if (autoPickSingle) {
      inputEl.value = picked;
    } else {
      if (event && (event.ctrlKey || event.metaKey)) {
        appendSellerValue(inputEl, picked);
      } else {
        inputEl.value = picked;
      }
    }
  });
  if (autoPickSingle && filtered.length === 1) {
    inputEl.value = filtered[0];
  }
};

searchFilter.addEventListener("input", () => {
  const filtered = renderOptionList(
    presets.search_phrases || [],
    searchFilter.value,
    searchOptions,
    (value) => {
      searchInput.value = value;
    }
  );
  if (filtered.length === 1 && searchFilter.value.trim()) {
    searchInput.value = filtered[0];
  }
});

sellerFilter.addEventListener("input", () => {
  const filtered = renderOptionList(
    presets.sellers || [],
    sellerFilter.value,
    sellerOptions,
    (value) => {
      sellerInput.value = value;
    }
  );
  if (filtered.length === 1 && sellerFilter.value.trim()) {
    sellerInput.value = filtered[0];
  }
});

searchInput.addEventListener("input", () => {
  autoSearchFromInput(searchInput, presets.search_phrases || [], searchFilter, searchOptions, true);
});

sellerInput.addEventListener("input", () => {
  autoSearchFromInput(sellerInput, presets.sellers || [], sellerFilter, sellerOptions, false);
});

marketplaceSelect.addEventListener("change", () => {
  const selected = marketplaceSelect.value;
  setMarketplaceTheme(selected);
  populateTsSelect(selected);
  fetchPresets(currentTsId);
});

tsSelect.addEventListener("change", () => {
  currentTsId = tsSelect.value;
  try {
    localStorage.setItem("ozonLastTsId", currentTsId);
  } catch (err) {
    // ignore
  }
  updateScenarioTitle(currentTsId);
  fetchPresets(currentTsId);
});

runBatchBtn.addEventListener("click", runBatch);

if (resumeBatchBtn) {
  resumeBatchBtn.addEventListener("click", () => {
    const lastJobId = localStorage.getItem("ozonLastJobId");
    if (!lastJobId) return;
    currentJobId = lastJobId;
    pollJob(lastJobId);
    setResumeVisible(false);
  });
}

if (runSearchBtn) {
  runSearchBtn.addEventListener("click", async () => {
    const searchValue = searchInput.value.trim();
    if (!searchValue) {
      if (searchInfo) {
        searchInfo.textContent = "Поиск: не заполнен поиск";
      }
      return;
    }
    lastSearchUrls = [];
    if (searchInfo) {
      searchInfo.textContent = "Поиск: ставлю задачу…";
    }
    if (searchXlsxLinkTop) {
      searchXlsxLinkTop.classList.remove("active");
    }
    runSearchBtn.disabled = true;
    try {
      const searchSettings = {
        fresh_profile: Boolean(searchFreshProfile?.checked),
      };
      const res = await fetch("/search-only", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          search: searchValue,
          seller: sellerInput.value.trim(),
          search_settings: searchSettings,
          meta: {
            marketplace: marketplaceSelect.value,
            ts_id: tsSelect.value,
            zone: zoneSelect.value,
            phone: phoneInput.value.trim(),
            search: searchInput.value.trim(),
            seller: sellerInput.value.trim(),
          },
        }),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) {
        if (searchInfo) {
          searchInfo.textContent = data.error || "Поиск: ошибка постановки";
        }
        return;
      }
      currentSearchJobId = data.job_id;
      setFormLocked(true);
      if (stopBtn) {
        stopBtn.disabled = false;
      }
      if (searchInfo) {
        searchInfo.textContent = `Поиск: задача ${data.job_id} (${data.total})`;
      }
      pollJob(data.job_id, { isSearchOnly: true });
    } catch (err) {
      if (searchInfo) {
        searchInfo.textContent = "Поиск: ошибка сети";
      }
    } finally {
      runSearchBtn.disabled = false;
    }
  });
}

if (stopBtn) {
  stopBtn.addEventListener("click", async () => {
    const targetJob = currentJobId || currentSearchJobId;
    if (!targetJob) return;
    try {
      await fetch(`/jobs/${targetJob}/stop`, { method: "POST" });
      setResumeVisible(true);
    } catch (err) {
      updateQueueInfo("Очередь: ошибка остановки");
    }
  });
}

document.querySelectorAll(".clear-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    const target = btn.dataset.clear;
    if (target === "search") {
      searchInput.value = "";
    }
    if (target === "seller") {
      sellerInput.value = "";
    }
  });
});

document.querySelectorAll(".clear-select").forEach((btn) => {
  btn.addEventListener("click", () => {
    const target = btn.dataset.clearSelect;
    const listEl = target === "ok" ? okRuleList : target === "error" ? errorRuleList : null;
    if (!listEl) return;
    listEl.querySelectorAll("input[type='checkbox']").forEach((el) => {
      el.checked = false;
    });
  });
});

document.querySelectorAll(".remove-select").forEach((btn) => {
  btn.addEventListener("click", () => {
    const target = btn.dataset.removeSelect;
    const listEl = target === "ok" ? okRuleList : target === "error" ? errorRuleList : null;
    const selectEl = target === "ok" ? okSelect : errorSelect;
    if (!listEl || !selectEl) return;
    const toRemove = Array.from(listEl.querySelectorAll("input[type='checkbox']:checked")).map(
      (el) => el.value
    );
    toRemove.forEach((value) => {
      const opt = Array.from(selectEl.options).find((o) => o.value === value);
      if (opt && opt.dataset.preset === "1") {
        return;
      }
      if (opt) {
        opt.remove();
      }
      const row = listEl.querySelector(`input[type='checkbox'][value="${value}"]`);
      if (row && row.parentElement) {
        row.parentElement.remove();
      }
    });
  });
});

tryRestoreJob();

document.querySelectorAll(".select-all").forEach((btn) => {
  btn.addEventListener("click", () => {
    const target = btn.dataset.selectAll;
    const listEl = target === "ok" ? okRuleList : target === "error" ? errorRuleList : null;
    if (!listEl) return;
    listEl.querySelectorAll("input[type='checkbox']").forEach((el) => {
      el.checked = true;
    });
  });
});

if (themeToggle) {
  themeToggle.addEventListener("change", () => {
    setTheme(themeToggle.checked ? "light" : "dark");
  });
}

[filterOk, filterNok, filterUnknown, filterError].forEach((el) => {
  if (!el) return;
  el.addEventListener("change", () => {
    renderResults(lastResults, null);
    updateResultStats(lastResults);
    updateXlsxLinkWithFilter(currentJobId);
  });
});

populateMarketplaceSelect();
populateTsSelect(marketplaceSelect.value);
initModeSwitches();
initRuleBlocks();
setMode("search", "preset");
setMode("seller", "preset");
loadTheme();
fetchPresets(currentTsId);
