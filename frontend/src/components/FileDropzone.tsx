import React, { useState, useRef, useCallback } from 'react';
import { Box, Flex, Text, Card } from '@radix-ui/themes';
import { UploadCloud, File, X } from 'lucide-react';
import { cn } from '@/utils/cn';

interface FileDropzoneProps {
  onFileSelect: (file: File | null) => void;
  accept?: string;
  label?: string;
  description?: string;
  className?: string;
}

export function FileDropzone({
  onFileSelect,
  accept,
  label = "Upload a file",
  description = "Drag and drop or click to select",
  className,
}: FileDropzoneProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);

    const files = e.dataTransfer.files;
    if (files && files.length > 0) {
      const file = files[0];
      setSelectedFile(file);
      onFileSelect(file);
    }
  }, [onFileSelect]);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      const file = files[0];
      setSelectedFile(file);
      onFileSelect(file);
    }
  };

  const clearFile = (e: React.MouseEvent) => {
    e.stopPropagation();
    setSelectedFile(null);
    onFileSelect(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const openFileDialog = () => {
    fileInputRef.current?.click();
  };

  return (
    <Box className={cn("w-full", className)}>
      <input
        type="file"
        ref={fileInputRef}
        onChange={handleFileChange}
        accept={accept}
        className="hidden"
      />
      
      <Card
        onClick={openFileDialog}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        className={cn(
          "relative border-2 border-dashed transition-all duration-200 cursor-pointer p-8",
          isDragging ? "border-blue-500 bg-blue-50/50" : "border-gray-300 hover:border-gray-400",
          selectedFile ? "bg-gray-50/50" : "bg-white"
        )}
      >
        <Flex direction="column" align="center" justify="center" gap="3">
          {selectedFile ? (
            <Flex direction="column" align="center" gap="2" className="w-full">
              <div className="p-3 bg-blue-100 rounded-full text-blue-600">
                <File size={32} />
              </div>
              <Flex direction="column" align="center" className="max-w-full overflow-hidden">
                <Text weight="bold" size="3" className="truncate w-full text-center">
                  {selectedFile.name}
                </Text>
                <Text size="1" color="gray">
                  {(selectedFile.size / 1024).toFixed(2)} KB
                </Text>
              </Flex>
              <button
                onClick={clearFile}
                className="mt-2 p-1.5 hover:bg-red-100 text-red-600 rounded-md transition-colors flex items-center gap-1.5 text-xs font-medium"
              >
                <X size={14} /> Remove file
              </button>
            </Flex>
          ) : (
            <>
              <div className={cn(
                "p-4 rounded-full transition-colors",
                isDragging ? "bg-blue-100 text-blue-600" : "bg-gray-100 text-gray-500"
              )}>
                <UploadCloud size={40} />
              </div>
              <Flex direction="column" align="center" gap="1">
                <Text weight="bold" size="3">
                  {label}
                </Text>
                <Text size="2" color="gray">
                  {description}
                </Text>
              </Flex>
            </>
          )}
        </Flex>
      </Card>
    </Box>
  );
}
