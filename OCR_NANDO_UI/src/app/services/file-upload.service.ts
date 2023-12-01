import { Injectable } from '@angular/core';
import { HttpClient, HttpRequest, HttpEvent } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class FileUploadService {
  private baseUrl = 'http://localhost:8000';

  constructor(private http: HttpClient) {}

  upload(imgName:string,imgBase64:string): Observable<HttpEvent<any>> {

    const payload:{}={
      "imgName":imgName,
      "imgBase64":imgBase64
    }

    const req = new HttpRequest('POST', `${this.baseUrl}/analysis/image`, payload, {
      reportProgress: true,
      responseType: 'json',
    });

    return this.http.request(req);
  }

  getImageResult(imgName:string): Observable<any> {
    return this.http.get(`${this.baseUrl}/analysis/image?name=${imgName}`);
  }
}