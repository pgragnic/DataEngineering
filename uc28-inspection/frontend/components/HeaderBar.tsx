"use client";

import { useEffect, useState } from "react";

type Variant = "dashboard" | "brief" | "capture" | "report";

interface Props {
  variant: Variant;
  title?: string;
  subtitle?: string;
  startedAt?: string | null;
  referential?: string;
  onReferentialChange?: (ref: string) => void;
}

function ElapsedTimer({ startedAt }: { startedAt: string }) {
  const [elapsed, setElapsed] = useState("00:00");

  useEffect(() => {
    const start = new Date(startedAt).getTime();
    const update = () => {
      const diff = Math.max(0, Math.floor((Date.now() - start) / 1000));
      const m = String(Math.floor(diff / 60)).padStart(2, "0");
      const s = String(diff % 60).padStart(2, "0");
      setElapsed(`${m}:${s}`);
    };
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [startedAt]);

  return <span className="font-mono text-uc-accent-lt text-sm font-bold">{elapsed}</span>;
}

const STATUS_PILL: Partial<Record<Variant, { label: string; className: string }>> = {
  brief: { label: "EN PRÉPARATION", className: "bg-uc-alert text-white" },
  report: { label: "AUDIT TERMINÉ", className: "bg-uc-accent text-white" },
};

export default function HeaderBar({
  variant,
  title = "BV·Inspect",
  subtitle,
  startedAt,
  referential = "ISO 9001",
  onReferentialChange,
}: Props) {
  const pill = STATUS_PILL[variant];

  return (
    <header className="h-[72px] bg-uc-bg-dark text-white flex items-center px-6 gap-4 shrink-0">
      <span className="font-bold text-lg tracking-wide text-uc-accent-lt">BV·INSPECT</span>
      <div className="flex-1 min-w-0">
        <p className="font-semibold text-sm truncate">{title}</p>
        {subtitle && <p className="text-xs text-uc-text-mute truncate">{subtitle}</p>}
      </div>
      {pill && (
        <span className={`px-3 py-1 rounded-full text-xs font-bold tracking-wider ${pill.className}`}>
          {pill.label}
        </span>
      )}
      {variant === "capture" && (
        <select
          className="bg-uc-bg-panel border border-uc-accent text-white text-sm rounded px-2 py-1"
          value={referential}
          onChange={(e) => {
            if (
              window.confirm(`Régénérer la check-list pour ${e.target.value} ?`)
            ) {
              onReferentialChange?.(e.target.value);
            }
          }}
        >
          <option>ISO 9001</option>
          <option>NFC 15-100</option>
          <option>ATEX</option>
        </select>
      )}
      {variant === "capture" && startedAt && <ElapsedTimer startedAt={startedAt} />}
      <div className="w-8 h-8 rounded-full bg-uc-primary-2 flex items-center justify-center text-xs font-bold">
        AM
      </div>
    </header>
  );
}
