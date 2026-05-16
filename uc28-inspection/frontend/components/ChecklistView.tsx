"use client";

import { useState } from "react";
import type { Checklist } from "@/lib/api";

interface Props {
  checklist: Checklist | null;
  activePointId?: string | null;
  validatedPointIds?: Set<string>;
  onSelectPoint?: (pointId: string) => void;
}

function PointMarker({ validated, active }: { validated: boolean; active: boolean }) {
  if (validated) return <span className="text-uc-accent font-bold">✓</span>;
  if (active) return <span className="text-uc-alert font-bold">◐</span>;
  return <span className="text-uc-text-mute">○</span>;
}

export default function ChecklistView({
  checklist,
  activePointId,
  validatedPointIds = new Set(),
  onSelectPoint,
}: Props) {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(["S1"]));

  if (!checklist) {
    return (
      <div className="flex items-center justify-center h-full text-uc-text-mute text-sm">
        Check-list non générée
      </div>
    );
  }

  const toggle = (id: string) => {
    setExpandedSections((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  return (
    <div className="flex flex-col gap-2 overflow-y-auto">
      <p className="text-xs font-bold text-uc-text-mute uppercase tracking-wider mb-1">
        Check-list dynamique
      </p>
      {checklist.sections.map((section) => {
        const isExpanded = expandedSections.has(section.id);
        return (
          <div key={section.id} className="rounded-lg border border-uc-border bg-white overflow-hidden">
            <button
              onClick={() => toggle(section.id)}
              className="w-full flex items-center gap-2 p-2 text-left hover:bg-uc-accent-50 transition-colors"
            >
              <span className="font-mono text-xs font-bold text-uc-primary">{section.id}</span>
              <span className="text-xs font-medium text-uc-text-body flex-1 truncate">
                {section.title}
              </span>
              <span className="text-uc-text-mute text-xs">{isExpanded ? "▾" : "▸"}</span>
            </button>
            {isExpanded && (
              <div className="border-t border-uc-border divide-y divide-uc-border">
                {section.points.map((point) => (
                  <button
                    key={point.id}
                    onClick={() => onSelectPoint?.(point.id)}
                    className={`w-full flex items-start gap-2 p-2 text-left hover:bg-uc-accent-50 transition-colors ${
                      activePointId === point.id ? "bg-uc-alert-50" : ""
                    }`}
                  >
                    <span className="mt-0.5 text-xs">
                      <PointMarker
                        validated={validatedPointIds.has(point.id)}
                        active={activePointId === point.id}
                      />
                    </span>
                    <span className="text-xs text-uc-text-body leading-snug">{point.question}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
        );
      })}
      <div className="mt-1 flex gap-3 text-xs text-uc-text-mute">
        <span><span className="text-uc-accent">✓</span> Validé</span>
        <span><span className="text-uc-alert">◐</span> Actif</span>
        <span>○ À faire</span>
      </div>
    </div>
  );
}
