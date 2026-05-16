"use client";

import { useRef } from "react";
import { uploadPhoto } from "@/lib/api";

interface Props {
  onUploaded: (photoId: string, previewUrl: string) => void;
}

export default function PhotoCapture({ onUploaded }: Props) {
  const inputRef = useRef<HTMLInputElement>(null);

  const handleChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const preview = URL.createObjectURL(file);
    const { id } = await uploadPhoto(file);
    onUploaded(id, preview);
    e.target.value = "";
  };

  return (
    <>
      <input
        ref={inputRef}
        type="file"
        accept="image/*"
        capture="environment"
        className="hidden"
        onChange={handleChange}
      />
      <button
        onClick={() => inputRef.current?.click()}
        className="flex items-center gap-2 px-4 py-2 bg-uc-accent text-white text-sm font-medium rounded-lg hover:bg-emerald-600 transition-colors"
        aria-label="Ajouter une photo"
      >
        📷 Ajouter photo
      </button>
    </>
  );
}
