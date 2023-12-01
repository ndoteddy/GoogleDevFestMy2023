import { Component, OnInit } from '@angular/core';
import { HttpEventType, HttpResponse } from '@angular/common/http';
import { Observable } from 'rxjs';
import { FileUploadService } from 'src/app/services/file-upload.service';

@Component({
  selector: 'app-image-upload',
  templateUrl: './image-upload.component.html',
  styleUrls: ['./image-upload.component.css']
})
export class ImageUploadComponent implements OnInit {
  selectedFiles?: FileList;
  currentFile?: File;
  progress = 0;
  message = '';
  preview = '';

  imageInfos?: Observable<any>;
  imageStructure?: Observable<any>;
  myInterval:any;
  constructor(private uploadService: FileUploadService) {}

  ngOnInit(): void {
  
  }

  selectFile(event: any): void {
    this.message = '';
    this.preview = '';
    this.progress = 0;
    this.selectedFiles = event.target.files;

    if (this.selectedFiles) {
      const file: File | null = this.selectedFiles.item(0);

      if (file) {
        this.preview = '';
        this.currentFile = file;

        const reader = new FileReader();

        reader.onload = (e: any) => {
          console.log(e.target.result);
          this.preview = e.target.result;
        };

        reader.readAsDataURL(this.currentFile);
      }
    }
  }

  upload(): void {
    
    if(this.myInterval)
      clearInterval(this.myInterval);
    this.progress = 0;
    this.imageInfos =undefined;
    if (this.selectedFiles) {
      const file: File | null = this.selectedFiles.item(0);

      if (file) {
        this.currentFile = file;

        let imgName = file.name;
        let imgBase64 = this.preview.split(',')[1];
        
        this.uploadService.upload(imgName,imgBase64).subscribe({
          next: (event: any) => {          

            if (event.type === HttpEventType.UploadProgress) {
              this.progress = Math.round((100 * event.loaded) / event.total);
            } else if (event instanceof HttpResponse) {
              this.message = JSON.stringify(event.body);
              this.myInterval=setInterval(() => {
                this.uploadService.getImageResult(imgName).subscribe({
                  next: (event: any) => {
                    this.imageInfos = event.imgResult;
                    this.imageStructure = event.imgStructure;
                  }});                
              }, 5000);
            }
          },
          error: (err: any) => {
            console.log(err);
            this.progress = 0;

            if (err.error && err.error.message) {
              this.message = err.error.message;
            } else {
              this.message = 'Could not upload the image!';
            }

            this.currentFile = undefined;
          },
        });
      }

      this.selectedFiles = undefined;
    }
  }
}