
import { motion } from "framer-motion";
import type { Constat } from "@/lib/api";
import NCBadge from "./NCBadge";

const BORDER_COLORS: Record<string, string> = {
  nc_majeure:  "border-uc-danger",
  nc_mineure:  "border-uc-alert",
  observation: "border-uc-primary-2",
  conforme:    "border-uc-accent",
};

export default function ConstatCard({ constat }: { constat: Constat }) {
  const borderColor = BORDER_COLORS[constat.classification] ?? "border-uc-border";

  return (
    <motion.div
      initial={{ x: 40, opacity: 0 }}
      animate={{ x: 0, opacity: 1 }}
      transition={{ duration: 0.3, type: "spring", bounce: 0.3 }}
      className={`bg-white rounded-lg shadow-sm border-l-4 ${borderColor} p-3 cursor-pointer hover:shadow-md transition-shadow`}
    >
      <div className="flex items-start justify-between gap-2 mb-1">
        <NCBadge level={constat.classification} />
        <div className="flex items-center gap-2">
          {constat.photo_path && (
            <span className="text-uc-text-mute text-xs">📷</span>
          )}
          {constat.norm_reference && (
            <span className="font-mono text-xs text-uc-text-mute truncate max-w-[120px]">
              {constat.norm_reference}
            </span>
          )}
        </div>
      </div>
      <p className="text-sm text-uc-text-body line-clamp-2">
        {constat.reformulated_text}
      </p>
    </motion.div>
  );
}
